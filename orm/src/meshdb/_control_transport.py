"""Private, single-attempt cluster administration transport."""
from __future__ import annotations

from http.client import HTTPConnection, HTTPSConnection, HTTPException, IncompleteRead
import ipaddress
import json
import math
import socket
import subprocess
import sys
import threading
import time
from urllib.parse import urlsplit
import uuid

from .client import (
    _Transport, _ERROR_STATUS, _ERROR_TYPES, _unique_object,
    _invalid_constant, _finite_float,
)
from .errors import (
    LimitExceededError, MeshDBError, ProtocolError, TransportError,
    UnknownOutcomeError, ValidationError,
)

_MAX_BODY = 1024 * 1024
_MUTATIONS = frozenset({'cluster.scale', 'cluster.autoscale'})
_OPERATIONS = _MUTATIONS | {'cluster.status'}
# An isolated resolver process is needed only for hostname deadlines. A blocked
# libc resolver cannot be cancelled safely in a Python thread. No background
# resolver survives return from this call.
_RESOLVE = '''import json,socket,sys
print(json.dumps(socket.getaddrinfo(sys.argv[1],int(sys.argv[2]),type=socket.SOCK_STREAM)))
'''


def _remaining(deadline: float | None, timeout: float) -> float:
    if deadline is None:
        return timeout
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError
    return min(timeout, remaining)


def _addresses(host: str, port: int, deadline: float | None):
    try:
        ipaddress.ip_address(host.split('%', 1)[0])
        numeric = True
    except ValueError:
        numeric = False
    if deadline is None or numeric:
        return socket.getaddrinfo(host, port, type=socket.SOCK_STREAM,
                                  flags=socket.AI_NUMERICHOST if numeric else 0)
    with subprocess.Popen([sys.executable, '-I', '-S', '-c', _RESOLVE, host, str(port)],
                          stdout=subprocess.PIPE, stderr=subprocess.DEVNULL) as resolver:
        try:
            while True:
                try:
                    # Selector timeout ranges can be smaller than Python floats.
                    output, _ = resolver.communicate(timeout=_remaining(deadline, 60.0))
                    break
                except subprocess.TimeoutExpired:
                    _remaining(deadline, float('inf'))
        except BaseException:
            resolver.kill()
            resolver.communicate()
            raise
        if resolver.returncode:
            raise OSError('Name resolution failed')
    _remaining(deadline, float('inf'))
    return [(family, kind, proto, name, tuple(address))
            for family, kind, proto, name, address in json.loads(output)]


class _Deadline:
    """Interrupt socket I/O even when a peer keeps trickling bytes."""

    def __init__(self, deadline: float | None):
        self.deadline = deadline
        self.socket = None
        self.lock = threading.Lock()
        self.cancelled = threading.Event()
        self.timer = None
        if deadline is not None:
            _remaining(deadline, float('inf'))
            self.timer = threading.Thread(target=self._wait)
            self.timer.name = 'meshdb-control-deadline'
            self.timer.daemon = True
            self.timer.start()

    def _wait(self):
        while not self.cancelled.is_set():
            remaining = self.deadline - time.monotonic()
            if remaining <= 0:
                self._expire()
                return
            # Native waits have a smaller range than finite Python floats.
            # Wake in bounded segments without moving the absolute deadline.
            if self.cancelled.wait(min(remaining, threading.TIMEOUT_MAX)):
                return

    def _expire(self):
        with self.lock:
            if self.socket is not None:
                try:
                    # Preserve SSLSocket state while interrupting its underlying I/O.
                    socket.socket.shutdown(self.socket, socket.SHUT_RDWR)
                except OSError:
                    pass

    def attach(self, sock):
        with self.lock:
            self.socket = sock
            _remaining(self.deadline, float('inf'))

    def close(self):
        if self.timer is not None:
            self.cancelled.set()
            self.timer.join()
        with self.lock:
            self.socket = None


class _ControlTransport(_Transport):
    """Validate locally, then send one bounded control envelope without retries.

    Ordinary timeout applies per blocking I/O. An optional monotonic deadline
    also interrupts the whole exchange. DNS under a deadline runs in a reaped
    subprocess so blocked name resolution cannot strand a resolver thread.
    """

    def __init__(self, api_url: str, admin_token: str, *, timeout: float = 30.0,
                 ca_file: str | None = None) -> None:
        super().__init__(api_url, admin_token, timeout=timeout, ca_file=ca_file)
        self._url = self._url[:-len('/v1/operations')] + '/v1/cluster/operations'
        self._context = next(h._context for h in self._opener.handlers
                             if hasattr(h, '_context'))

    def request(self, operation: str, payload: dict, *, request_id: str | None = None,
                deadline: float | None = None) -> tuple[str, object]:
        if not isinstance(operation, str) or operation not in _OPERATIONS:
            raise ValidationError('Unsupported control operation')
        if not isinstance(payload, dict):
            raise ValidationError('Operation payload must be a dictionary')
        if request_id is None:
            request_id = str(uuid.uuid4())
        else:
            try:
                if not isinstance(request_id, str) or str(uuid.UUID(request_id)) != request_id.lower():
                    raise ValueError
            except (ValueError, AttributeError):
                raise ValidationError('Request ID must be a UUID string') from None
        if deadline is not None:
            try:
                if type(deadline) not in (int, float) or not math.isfinite(deadline):
                    raise ValueError
                deadline = float(deadline)
            except (ValueError, OverflowError):
                raise ValidationError('Deadline must be a finite monotonic time') from None
        try:
            body = json.dumps({'request_id': request_id, 'operation': operation, 'payload': payload},
                              ensure_ascii=False, allow_nan=False, separators=(',', ':')).encode('utf-8')
        except (ValueError, TypeError, UnicodeError, RecursionError):
            raise ValidationError('Operation payload must encode as finite UTF-8 JSON',
                                  request_id=request_id) from None
        if len(body) > _MAX_BODY:
            raise LimitExceededError('Encoded control request exceeds 1 MiB', request_id=request_id)
        try:
            # JSON allows non-string Python keys to collide after encoding.
            json.loads(body, object_pairs_hook=_unique_object)
        except (ValueError, RecursionError):
            raise ValidationError('Operation payload contains duplicate JSON fields',
                                  request_id=request_id) from None
        status = None
        possibly_sent = False
        guard = None
        connection = None
        response = None

        def failure(message, *, network=False):
            error_type = (UnknownOutcomeError if possibly_sent and operation in _MUTATIONS
                          else TransportError if network else ProtocolError)
            return error_type(message, request_id=request_id, status_code=status)

        try:
            guard = _Deadline(deadline)
            url = urlsplit(self._url)
            port = url.port if url.port is not None else (443 if url.scheme == 'https' else 80)
            connection = (HTTPSConnection(url.hostname, port, context=self._context)
                          if url.scheme == 'https' else HTTPConnection(url.hostname, port))
            # Connect before marking the request possibly sent. Neither DNS,
            # TCP connection setup nor the TLS handshake submits a policy.
            addresses = _addresses(url.hostname, port, deadline)
            last_error = None
            for family, kind, proto, _, address in addresses:
                sock = socket.socket(family, kind, proto)
                try:
                    guard.attach(sock)
                    sock.settimeout(_remaining(deadline, self._timeout))
                    sock.connect(address)
                    if url.scheme == 'https':
                        # Attach the wrapped socket before starting a cancellable handshake.
                        sock = self._context.wrap_socket(sock, server_hostname=url.hostname,
                                                         do_handshake_on_connect=False)
                        guard.attach(sock)
                        sock.settimeout(_remaining(deadline, self._timeout))
                        sock.do_handshake()
                    connection.sock = sock
                    break
                except BaseException as error:
                    sock.close()
                    if not isinstance(error, OSError):
                        raise
                    last_error = error
                    _remaining(deadline, self._timeout)
            if connection.sock is None:
                raise last_error or OSError('No connection address')
            connection.sock.settimeout(_remaining(deadline, self._timeout))
            possibly_sent = True
            connection.request('POST', url.path, body=body, headers={
                'Content-Type': 'application/json', 'Accept': 'application/json',
                'Authorization': 'Bearer ' + self._token,
            })
            response = connection.getresponse()
            status = response.status
            raw = response.read(_MAX_BODY + 1)
            content_type = response.headers.get('Content-Type', '').split(';', 1)[0].strip().lower()
            length = response.headers.get('Content-Length')
            _remaining(deadline, self._timeout)
        except (OSError, IncompleteRead, subprocess.SubprocessError):
            raise failure('Control response lost during HTTP I/O', network=True) from None
        except HTTPException:
            raise failure('Malformed control HTTP response') from None
        finally:
            # Stop and join cancellation before closing/reusing socket descriptors.
            if guard is not None:
                guard.close()
            if response is not None:
                response.close()
            if connection is not None:
                connection.close()
        if len(raw) > _MAX_BODY:
            raise failure('Control response exceeds 1 MiB')
        if length is not None:
            try:
                if not length.isascii() or not length.isdecimal():
                    raise ValueError
                declared_length = int(length)
            except ValueError:
                raise failure('Invalid control response length') from None
            if declared_length != len(raw):
                raise failure('Incomplete control response body', network=True)
        if content_type != 'application/json':
            raise failure('Control response is not application/json')
        try:
            envelope = json.loads(raw.decode('utf-8'), object_pairs_hook=_unique_object,
                                  parse_constant=_invalid_constant, parse_float=_finite_float)
        except (ValueError, UnicodeError, RecursionError):
            raise failure('Malformed control response JSON') from None
        if not isinstance(envelope, dict):
            raise failure('Invalid control response envelope')
        if status == 200:
            if set(envelope) != {'request_id', 'data'} or envelope['request_id'] != request_id:
                raise failure('Invalid control success envelope or request ID')
            try:
                _remaining(deadline, self._timeout)
            except TimeoutError:
                raise failure('Control deadline expired', network=True) from None
            return request_id, envelope['data']
        if not 400 <= status <= 599:
            raise failure('Invalid control response status')
        if (set(envelope) != {'request_id', 'error'}
                or envelope['request_id'] not in (None, request_id)):
            raise failure('Invalid control error envelope or request ID')
        error = envelope['error']
        if (not isinstance(error, dict) or set(error) != {'code', 'message', 'outcome'}
                or not isinstance(error['code'], str) or not error['code']
                or not isinstance(error['message'], str)
                or error['outcome'] not in ('not_applied', 'unknown')):
            raise failure('Invalid control error details')
        code, outcome = error['code'], error['outcome']
        expected_status = _ERROR_STATUS.get(code)
        if ((expected_status is not None and status != expected_status)
                or (code == 'outcome_unknown' and outcome != 'unknown')):
            raise failure('Control error code/status/outcome mismatch')
        error_type = UnknownOutcomeError if outcome == 'unknown' else _ERROR_TYPES.get(code, MeshDBError)
        # Error messages are untrusted and may echo credentials or request data.
        raise error_type('Control operation rejected', code=code, request_id=request_id,
                         status_code=status, outcome=outcome)
