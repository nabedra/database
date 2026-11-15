"""Private bounded single-request HTTP ingress around the reviewed callback pool."""
from __future__ import annotations

from collections import deque
import hmac
import re
import socket
import threading
import time

from ._callback_pool import _CallbackPool
from ._callback_protocol import (
    _CallbackProtocolError, _MAX_BODY, _MAX_HEADERS, _bounded_json, _decode_command,
)

_IO_TIMEOUT = 2.0


def _close_socket(peer):
    try:
        peer.shutdown(socket.SHUT_RDWR)
    except OSError:
        pass
    peer.close()


def _budget(peer, deadline):
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError
    peer.settimeout(remaining)


def _reject(code='invalid_command'):
    raise _CallbackProtocolError(code)


class _CallbackBridge:
    """Own one port-zero listener, eight handlers and one _CallbackPool.

    Inputs are already validated callbacks and independent lifetime tokens.
    url is the actual startup callback address. activate takes prepared, validated
    cluster/authority bindings. stop_admission is terminal and does no joins.
    close joins ingress for <=1s total then calls pool.close exactly once, whose
    callback joins total <=2s. alive covers all original threads after any close.
    The future process guard must retain this owner while alive is true.
    """

    def __init__(self, callbacks, callback_token, authorize_token, *, _register_owner=None):
        self.__callback_token = ('Bearer ' + callback_token).encode('ascii')
        self._condition = threading.Condition()
        self._stopping = False
        self._pending = deque()
        self._sockets = set()
        self._threads = []
        self._pool = None
        self._listener = None
        if _register_owner is not None:
            _register_owner(self)
        failed = False
        try:
            self._listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._listener.bind(('127.0.0.1', 0))
            self._listener.listen(8)
            self._listener.settimeout(.1)
            port = self._listener.getsockname()[1]
            self._host = f'127.0.0.1:{port}'.encode('ascii')
            self.url = f'http://127.0.0.1:{port}/private/v1/callback'
            _CallbackPool(callbacks, authorize_token, _register_owner=self._retain_pool)
            for index in range(8):
                self._start(self._run, f'meshdb-callback-http-{index}')
            self._start(self._accept, 'meshdb-callback-accept')
        except BaseException:
            failed = True
        if failed:
            self.close()
            # Outside the exception handler: no raw OS/secret exception context.
            raise RuntimeError('Callback bridge startup failed') from None

    def _retain_pool(self, pool):
        # Retain before startup: a raising constructor never returns its owner.
        self._pool = pool

    def _start(self, target, name):
        thread = threading.Thread(target=target, name=name, daemon=True)
        self._threads.append(thread)
        thread.start()

    @property
    def alive(self):
        return (any(thread.is_alive() for thread in self._threads) or
                (self._pool is not None and self._pool.alive))

    def activate(self, cluster_id, authorize_url):
        self._pool.activate(cluster_id, authorize_url)

    def stop_admission(self):
        if self._pool is not None:
            self._pool.stop_admission()
        with self._condition:
            self._stopping = True
            if self._listener is not None:
                _close_socket(self._listener)
            for peer in self._sockets:
                _close_socket(peer)
            self._sockets.clear()
            self._pending.clear()
            self._condition.notify_all()

    def close(self):
        self.stop_admission()
        deadline = time.monotonic() + 1.0
        try:
            for thread in self._threads:
                if thread.ident is not None and thread is not threading.current_thread():
                    thread.join(max(0, deadline-time.monotonic()))
        finally:
            if self._pool is not None:
                self._pool.close()

    def _accept(self):
        while True:
            try:
                peer, address = self._listener.accept()
                deadline = time.monotonic() + _IO_TIMEOUT
            except socket.timeout:
                continue
            except OSError:
                return
            with self._condition:
                if self._stopping or address[0] != '127.0.0.1' or len(self._pending) == 8:
                    _close_socket(peer)
                else:
                    self._sockets.add(peer)
                    self._pending.append((peer, deadline))
                    self._condition.notify()
                if self._stopping:
                    return

    def _run(self):
        while True:
            with self._condition:
                while not self._pending and not self._stopping:
                    self._condition.wait()
                if self._stopping:
                    return
                peer, deadline = self._pending.popleft()
            try:
                self._handle(peer, deadline)
            except (OSError, ValueError):
                # Lost sockets never cancel an entered pool call.
                pass
            finally:
                with self._condition:
                    self._sockets.discard(peer)
                    _close_socket(peer)

    def _headers(self, peer, deadline):
        received = bytearray()
        while b'\r\n\r\n' not in received:
            if len(received) == _MAX_HEADERS:
                _reject('limit_exceeded')
            _budget(peer, deadline)
            # Do not consume even a coalesced body before credential comparison.
            chunk = peer.recv(1)
            if not chunk:
                _reject()
            received.extend(chunk)
        head, initial = bytes(received).split(b'\r\n\r\n', 1)
        lines = head.split(b'\r\n')
        if lines[0] != b'POST /private/v1/callback HTTP/1.1':
            _reject()
        headers = {}
        for line in lines[1:]:
            key, colon, value = line.partition(b':')
            if (not colon or re.fullmatch(rb"[!#$%&'*+.^_`|~0-9A-Za-z-]+", key) is None or
                    any(c < 32 and c != 9 or c == 127 for c in value)):
                _reject()
            key = key.lower()
            if key in headers:
                _reject()
            headers[key] = value.strip(b' \t')
        if not hmac.compare_digest(headers.get(b'authorization', b''), self.__callback_token):
            _reject('unauthorized')
        if (headers.get(b'host') != self._host or
                headers.get(b'content-type', b'').lower() != b'application/json' or
                any(key in headers for key in (b'transfer-encoding', b'content-encoding',
                                               b'expect', b'upgrade')) or
                headers.get(b'connection', b'close').lower() not in (b'close', b'keep-alive')):
            _reject()
        length = headers.get(b'content-length', b'')
        if re.fullmatch(rb'0|[1-9][0-9]{0,6}', length) is None:
            _reject()
        length = int(length)
        if length > _MAX_BODY:
            _reject('limit_exceeded')
        # Extra buffered bytes are discarded; never parse a second request.
        return length, initial[:length]

    def _handle(self, peer, deadline):
        try:
            length, initial = self._headers(peer, deadline)
            body = bytearray(initial)
            while len(body) < length:
                _budget(peer, deadline)
                chunk = peer.recv(min(65536, length-len(body)))
                if not chunk:
                    _reject()
                body.extend(chunk)
            # Even a completely queued request must satisfy its acceptance budget.
            _budget(peer, deadline)
            command = _decode_command(bytes(body))
        except (_CallbackProtocolError, TimeoutError) as exc:
            code = exc.code if isinstance(exc, _CallbackProtocolError) else 'invalid_command'
            status = {'unauthorized': 401, 'limit_exceeded': 413}.get(code, 400)
            raw = _bounded_json(dict(version=1, invocation_id=None,
                                     error=dict(code=code, outcome='not_invoked')))
        else:
            status, raw = self._pool.wait(self._pool.submit(command))
        self._write(peer, status, raw)

    def _write(self, peer, status, raw):
        deadline = time.monotonic() + _IO_TIMEOUT
        header = (f'HTTP/1.1 {status} Reply\r\nContent-Type: application/json\r\n'
                  f'Content-Length: {len(raw)}\r\nConnection: close\r\n\r\n').encode('ascii')
        for part in (header, raw):
            view = memoryview(part)
            while view:
                _budget(peer, deadline)
                count = peer.send(view[:65536])
                if not count:
                    return
                view = view[count:]
