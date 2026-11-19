"""Bounded RESP2 transport for one in-flight request per connection."""
from __future__ import annotations

from dataclasses import dataclass
import ipaddress
from queue import Empty, SimpleQueue
import re
import socket
import ssl
from threading import BoundedSemaphore, Lock, Thread
import time

from .errors import OverloadedError, ProtocolError, TransportError


MAX_VALUE = 1024 * 1024
_MAX_REPLY = 8 * MAX_VALUE
_READ_SIZE = 64 * 1024
_MAX_LINE = 4096
_MAX_ITEMS = 262_144
_INTEGER = re.compile(rb"-?[0-9]{1,19}")
_DNS_SLOTS = BoundedSemaphore(16)


@dataclass(frozen=True, slots=True)
class Endpoint:
    tls: bool
    host: str
    port: int


@dataclass(frozen=True, slots=True)
class ErrorReply:
    text: bytes


def remaining(deadline: float) -> float:
    seconds = deadline - time.monotonic()
    if seconds <= 0:
        raise TransportError("cache deadline expired", outcome="not_applied")
    return seconds


def _loopback(host: str) -> bool:
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def _resolve(endpoint: Endpoint, deadline: float) -> list:
    try:
        ipaddress.ip_address(endpoint.host)
        flags = socket.AI_NUMERICHOST
    except ValueError:
        flags = 0
    if flags:
        return socket.getaddrinfo(endpoint.host, endpoint.port, type=socket.SOCK_STREAM, flags=flags)

    # getaddrinfo has no portable timeout. A stuck resolver retains its permit.
    if not _DNS_SLOTS.acquire(blocking=False):
        raise OverloadedError("cache DNS resolver is busy")
    result: SimpleQueue = SimpleQueue()

    def resolve() -> None:
        try:
            result.put(socket.getaddrinfo(endpoint.host, endpoint.port, type=socket.SOCK_STREAM))
        except Exception as error:
            result.put(error)
        finally:
            _DNS_SLOTS.release()

    try:
        Thread(target=resolve, daemon=True).start()
    except RuntimeError:
        _DNS_SLOTS.release()
        raise TransportError("cache DNS resolver could not start", outcome="not_applied") from None
    try:
        response = result.get(timeout=remaining(deadline))
    except Empty:
        raise TransportError("cache DNS deadline expired", outcome="not_applied") from None
    if isinstance(response, Exception):
        raise response
    return response


class Connection:
    def __init__(self):
        self._socket: socket.socket | None = None
        self._closed = False
        self._lifecycle = Lock()
        self.buffer = b""
        self.offset = 0
        self.received = 0
        self.items = 0

    def connect(self, endpoint: Endpoint, context: ssl.SSLContext | None, deadline: float) -> None:
        try:
            addresses = _resolve(endpoint, deadline)
            for family, kind, protocol, _, address in addresses:
                if not endpoint.tls and not _loopback(address[0]):
                    continue
                with self._lifecycle:
                    self._check_open()
                    sock = self._socket = socket.socket(family, kind, protocol)
                try:
                    sock.settimeout(remaining(deadline))
                    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                    sock.connect(address)
                    if endpoint.tls:
                        assert context is not None
                        with self._lifecycle:
                            self._check_open()
                            # Transfer socket ownership before the blocking handshake.
                            sock = self._socket = context.wrap_socket(
                                sock, server_hostname=endpoint.host, do_handshake_on_connect=False,
                            )
                        sock.settimeout(remaining(deadline))
                        sock.do_handshake()
                    return
                except OSError:
                    sock.close()
                except BaseException:
                    sock.close()
                    raise
        except OSError:
            pass
        raise TransportError("cache connection or TLS verification failed", outcome="not_applied")

    def _check_open(self) -> None:
        if self._closed:
            raise TransportError("cache connection closed", outcome="not_applied")

    def close(self) -> None:
        with self._lifecycle:
            if self._closed:
                return
            self._closed = True
            if sock := self._socket:
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                sock.close()

    def exchange(self, args: tuple[bytes, ...], deadline: float, submit=None):
        sock = self._socket
        assert sock is not None, "cache exchange requires a connected socket"
        frame = [f"*{len(args)}\r\n".encode()]
        for arg in args:
            frame.extend((f"${len(arg)}\r\n".encode(), arg, b"\r\n"))
        payload = b"".join(frame)
        sock.settimeout(remaining(deadline))
        if submit is not None:
            submit()
        sock.sendall(payload)
        self.buffer = b""
        self.offset = self.received = self.items = 0
        reply = self._read(deadline)
        if self.offset != len(self.buffer):
            raise ProtocolError("extra bytes after cache reply")
        self.buffer = b""
        return reply

    def _fetch(self, size: int, deadline: float) -> bytes:
        sock = self._socket
        assert sock is not None, "cache reply requires a connected socket"
        sock.settimeout(remaining(deadline))
        chunk = sock.recv(size)
        if not chunk:
            raise TransportError("cache peer closed before a complete reply")
        self.received += len(chunk)
        if self.received > _MAX_REPLY:
            raise ProtocolError("cache reply exceeds byte limit")
        return chunk

    def _line(self, deadline: float) -> bytes:
        while True:
            end = self.buffer.find(b"\r\n", self.offset)
            if end >= 0:
                if end - self.offset > _MAX_LINE:
                    raise ProtocolError("cache reply line exceeds byte limit")
                line = self.buffer[self.offset:end]
                self.offset = end + 2
                return line
            if len(self.buffer) - self.offset > _MAX_LINE + 1:
                raise ProtocolError("cache reply line exceeds byte limit")
            self.buffer = self.buffer[self.offset:] + self._fetch(_READ_SIZE, deadline)
            self.offset = 0

    def _bulk(self, size: int, deadline: float) -> bytes:
        end = self.offset + size
        if end + 2 <= len(self.buffer):
            value = self.buffer[self.offset:end]
            if self.buffer[end:end + 2] != b"\r\n":
                raise ProtocolError("malformed cache bulk reply")
            self.offset = end + 2
            return value

        # Allocate once for a split body, after validating its declared length.
        value = bytearray(size + 2)
        tail = self.buffer[self.offset:]
        value[:len(tail)] = tail
        self.offset = len(self.buffer)
        copied = len(tail)
        while copied < len(value):
            chunk = self._fetch(min(_READ_SIZE, len(value) - copied), deadline)
            value[copied:copied + len(chunk)] = chunk
            copied += len(chunk)
        if value[-2:] != b"\r\n":
            raise ProtocolError("malformed cache bulk reply")
        return bytes(memoryview(value)[:size])

    def _read(self, deadline: float, depth: int = 0):
        self.items += 1
        if depth > 6 or self.items > _MAX_ITEMS:
            raise ProtocolError("cache reply exceeds nesting or item limit")
        line = self._line(deadline)
        if not line:
            raise ProtocolError("empty cache reply line")
        marker, body = line[:1], line[1:]
        if marker == b"+":
            return body.decode("ascii", "strict")
        if marker == b"-" and depth == 0:
            return ErrorReply(body)
        if marker not in (b":", b"$", b"*") or not _INTEGER.fullmatch(body):
            raise ProtocolError("malformed cache RESP2 reply")
        number = int(body)
        if marker == b":" and -(2**63) <= number < 2**63:
            return number
        if marker == b"$":
            if number == -1:
                return None
            if 0 <= number <= MAX_VALUE:
                return self._bulk(number, deadline)
        if marker == b"*" and 0 <= number <= _MAX_ITEMS - self.items:
            return [self._read(deadline, depth + 1) for _ in range(number)]
        raise ProtocolError("invalid cache reply size or integer")
