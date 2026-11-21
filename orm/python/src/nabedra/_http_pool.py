"""Reuse of idle HTTP/1.1 connections for one API origin.

A connection returns to the pool only after its response body was read to the
end and the server did not ask to close it. On checkout, a connection that has
been idle too long, or whose socket shows EOF or unexpected data, is closed
before any byte of the next request is written; opening a new connection in
its place sends the request once, so it is not a retry. After the first request
byte is written, every failure closes the connection and propagates. Nothing
here ever sends a request twice.
"""
from __future__ import annotations

from http.client import HTTPConnection, HTTPResponse, HTTPSConnection
import os
import select
import ssl
import threading
import time
import weakref
from urllib.request import Request

MAX_IDLE_CONNECTIONS = 4
# Below the server's 30 s idle bound for kept-alive API connections, so a reused
# connection is unlikely to race the server's idle close. A connection the
# server closed earlier (a full listener evicts its oldest idle connection)
# shows EOF and is replaced before any byte is written.
MAX_IDLE_SECONDS = 25.0


def _idle_socket_quiet(sock) -> bool:
    """True when an idle socket has no buffered data, EOF or error pending."""
    if sock is None:
        return False
    try:
        if isinstance(sock, ssl.SSLSocket) and sock.pending():
            return False
        if hasattr(select, 'poll'):
            poller = select.poll()
            # POLLHUP, POLLERR and POLLNVAL are reported without registration.
            poller.register(sock, select.POLLIN | select.POLLPRI)
            return not poller.poll(0)
        return not select.select([sock], [], [], 0)[0]
    except (OSError, ValueError):
        return False


class ConnectionPool:
    """Thread-safe idle connections to one scheme, host and port.

    `open(request, timeout=...)` mirrors `OpenerDirector.open` for the private
    transport: it sends one urllib `Request` without redirects or proxies and
    returns a response whose `close()` either pools or closes the connection.
    """

    def __init__(self, scheme: str, host: str, port: int | None,
                 context: ssl.SSLContext, *, max_idle: int = MAX_IDLE_CONNECTIONS,
                 max_idle_seconds: float = MAX_IDLE_SECONDS) -> None:
        self._https = scheme == 'https'
        self._host = host
        self._port = port
        self._context = context
        self._max_idle = max_idle
        self._max_idle_seconds = max_idle_seconds
        self._lock = threading.Lock()
        self._idle: list[tuple[HTTPConnection, float]] = []
        self._pid = os.getpid()
        if hasattr(os, 'register_at_fork'):
            # A lock held by another thread at fork time would block the child forever.
            # The hook holds a weak reference so it does not keep discarded pools alive.
            pool = weakref.ref(self)
            os.register_at_fork(after_in_child=lambda: _reset_after_fork(pool()))

    def _connect(self, timeout: float) -> HTTPConnection:
        if self._https:
            return HTTPSConnection(self._host, self._port, timeout=timeout, context=self._context)
        return HTTPConnection(self._host, self._port, timeout=timeout)

    def _checkout(self, timeout: float) -> HTTPConnection:
        while True:
            inherited = []
            with self._lock:
                if self._pid != os.getpid():
                    # A forked child must not share TLS state with its parent.
                    inherited = [connection for connection, _ in self._idle]
                    self._idle.clear()
                    self._pid = os.getpid()
                entry = self._idle.pop() if self._idle else None
            for connection in inherited:
                connection.close()
            if entry is None:
                return self._connect(timeout)
            connection, released = entry
            if (time.monotonic() - released < self._max_idle_seconds
                    and _idle_socket_quiet(connection.sock)):
                connection.timeout = timeout
                connection.sock.settimeout(timeout)
                return connection
            connection.close()

    def release(self, connection: HTTPConnection) -> None:
        with self._lock:
            if self._pid == os.getpid() and len(self._idle) < self._max_idle:
                self._idle.append((connection, time.monotonic()))
                return
        connection.close()

    def close(self) -> None:
        with self._lock:
            idle = [connection for connection, _ in self._idle]
            self._idle.clear()
        for connection in idle:
            connection.close()

    def open(self, request: Request, *, timeout: float) -> PooledResponse:
        connection = self._checkout(timeout)
        try:
            # Title-case names as urllib does, so the wire headers are unchanged.
            headers = {name.title(): value for name, value in request.header_items()}
            connection.request(request.get_method(), request.selector, body=request.data,
                               headers=headers)
            response = connection.getresponse()
        except BaseException:
            connection.close()
            raise
        return PooledResponse(self, connection, response)


class PooledResponse:
    """The parts of an HTTP response the transport reads, plus pooled close()."""

    def __init__(self, pool: ConnectionPool, connection: HTTPConnection,
                 response: HTTPResponse) -> None:
        self._pool = pool
        self._connection: HTTPConnection | None = connection
        self._response = response
        self._intact = True

    @property
    def status(self) -> int:
        return self._response.status

    @property
    def headers(self):
        return self._response.headers

    def read(self, amount: int) -> bytes:
        try:
            return self._response.read(amount)
        except BaseException:
            self._intact = False
            raise

    def close(self) -> None:
        connection, self._connection = self._connection, None
        if connection is None:
            return
        response = self._response
        # isclosed() is also true after an early EOF, so require the framing
        # to show the whole body arrived.
        reusable = (self._intact and response.isclosed() and not response.will_close
                    and (response.chunked or response.length == 0))
        response.close()
        if reusable:
            self._pool.release(connection)
        else:
            connection.close()


def _reset_after_fork(pool: ConnectionPool | None) -> None:
    if pool is not None:
        pool._lock = threading.Lock()
