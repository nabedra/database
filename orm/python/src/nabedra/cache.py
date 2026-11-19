"""Synchronous binary Cache API for Nabedra RESP2 endpoints."""
from __future__ import annotations

import base64
from binascii import crc_hqx
import ipaddress
from pathlib import Path
import re
import ssl
from threading import Lock
import time
from typing import Literal, Sequence

from ._cache_wire import Connection, Endpoint, ErrorReply, MAX_VALUE, remaining
from .errors import (
    AuthenticationError, OverloadedError, ProtocolError, StorageError,
    TransportError, UnknownOutcomeError, ValidationError,
)

__all__ = ["Cache", "CacheSetCondition"]

CacheSetCondition = Literal["always", "absent", "present"]
_MAX_TTL = 2**53 - 1
_SLOT_COUNT = 16384
_MAX_REDIRECTS = 4
_NAMESPACE = re.compile(r"[a-z0-9_-]{1,64}")
_LABEL = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?")
_URI = re.compile(r"(rediss?)://([^/?#]+)/?")
_AUTHORITY = re.compile(r"(\[[a-fA-F0-9:.]+\]|[a-zA-Z0-9.-]+)(?::([0-9]{1,5}))?")
_MOVED = re.compile(rb"MOVED ([0-9]{1,5}) ([-a-zA-Z0-9.:\[\]]+)")


def _checked_number(value: int, low: int, high: int, name: str) -> int:
    if type(value) is not int or not low <= value <= high:
        raise ValidationError(f"{name} must be an integer from {low} through {high}")
    return value


def _hostname(host: str) -> str:
    host = host.lower()
    try:
        return str(ipaddress.ip_address(host))
    except ValueError:
        if len(host) <= 253 and all(_LABEL.fullmatch(part) for part in host.split(".")):
            return host
        raise ValidationError("invalid cache hostname") from None


def _authority(authority: str, tls: bool, *, require_port: bool = False) -> Endpoint:
    match = _AUTHORITY.fullmatch(authority)
    if not match or (require_port and match[2] is None):
        raise ValidationError("invalid cache endpoint authority")
    name = match[1].strip("[]")
    if match[1].startswith("["):
        try:
            ipaddress.IPv6Address(name)
        except ValueError:
            raise ValidationError("invalid cache IPv6 address") from None
    host = _hostname(name)
    if not tls:
        try:
            loopback = ipaddress.ip_address(host).is_loopback
        except ValueError:
            loopback = host == "localhost"
        if not loopback:
            raise ValidationError("cache plaintext requires a loopback endpoint")
    return Endpoint(tls, host, _checked_number(int(match[2] or "6379"), 1, 65535, "port"))


def _endpoint(uri: str) -> Endpoint:
    match = _URI.fullmatch(uri) if isinstance(uri, str) and len(uri) <= 512 else None
    if not match:
        raise ValidationError("cache endpoint must be a redis:// or rediss:// URI")
    return _authority(match[2], match[1] == "rediss")


def _endpoints(values: str | Sequence[str], maximum: int) -> tuple[Endpoint, ...]:
    if isinstance(values, str):
        values = (values,)
    if not isinstance(values, Sequence) or len(values) > maximum:
        raise ValidationError("invalid cache endpoint list")
    return tuple(dict.fromkeys(_endpoint(value) for value in values))


def _credential(value: str, name: str) -> bytes:
    if isinstance(value, str) and 1 <= len(value) <= 4096:
        try:
            encoded = value.encode("utf-8")
            if len(encoded) <= 4096:
                return encoded
        except UnicodeEncodeError:
            pass
    raise ValidationError(f"invalid cache {name}")


def _reply_error(reply: ErrorReply):
    code = reply.text.partition(b" ")[0]
    if code in (b"NOAUTH", b"WRONGPASS", b"NOPERM"):
        return AuthenticationError("cache authentication or permission denied")
    if code in (b"BUSY", b"OOM", b"TRYAGAIN"):
        return OverloadedError("cache operation rejected before execution")
    if code == b"CLUSTERDOWN":
        return StorageError("cache slot unavailable")
    if code in (b"ERR", b"CROSSSLOT"):
        return ValidationError("cache command rejected")
    return ProtocolError("unrecognized cache error reply")


def _boolean(reply) -> bool:
    if type(reply) is int and reply in (0, 1):
        return bool(reply)
    raise ProtocolError("invalid cache boolean reply")


class _Pool:
    def __init__(self, size: int, context: ssl.SSLContext | None, auth: tuple[bytes, ...]):
        self.size = size
        self.context = context
        self.auth = auth
        self.lock = Lock()
        self.closed = False
        self.opening: set[Connection] = set()
        self.live: set[Connection] = set()
        self.idle: list[tuple[Endpoint, Connection]] = []

    def take(self, endpoint: Endpoint, deadline: float) -> Connection:
        remaining(deadline)
        with self.lock:
            if self.closed:
                raise TransportError("cache is closed", outcome="not_applied")
            for index in range(len(self.idle) - 1, -1, -1):
                if self.idle[index][0] == endpoint:
                    return self.idle.pop(index)[1]
            if len(self.opening) + len(self.live) >= self.size:
                if not self.idle:
                    raise OverloadedError("cache connection pool is busy")
                _, discarded = self.idle.pop(0)
                self.live.remove(discarded)
                discarded.close()
            connection = Connection()
            self.opening.add(connection)

        try:
            connection.connect(endpoint, self.context, deadline)
            if self.auth:
                reply = connection.exchange(self.auth, deadline)
                if isinstance(reply, ErrorReply):
                    raise _reply_error(reply)
                if reply != "OK":
                    raise ProtocolError("invalid cache AUTH reply")
            with self.lock:
                if self.closed:
                    raise TransportError("cache is closed", outcome="not_applied")
                self.live.add(connection)
                self.opening.remove(connection)
                return connection
        except BaseException as error:
            connection.close()
            with self.lock:
                self.opening.discard(connection)
            if isinstance(error, OSError):
                raise TransportError("cache authentication transport failed", outcome="not_applied") from error
            if isinstance(error, UnicodeError):
                raise ProtocolError("malformed cache AUTH reply", outcome="not_applied") from error
            if isinstance(error, (TransportError, ProtocolError)):
                error.outcome = "not_applied"
            raise

    def release(self, endpoint: Endpoint, connection: Connection, reusable: bool) -> None:
        with self.lock:
            if connection not in self.live:
                return
            if reusable:
                self.idle.append((endpoint, connection))
            else:
                self.live.remove(connection)
                connection.close()

    def close(self) -> None:
        with self.lock:
            self.closed = True
            for connection in self.opening:
                connection.close()
            for connection in self.live:
                connection.close()
            self.opening.clear()
            self.live.clear()
            self.idle.clear()


class Cache:
    """A connection pool for one namespace on a standalone node or a cluster."""

    @classmethod
    def connect(
        cls,
        endpoints: str | Sequence[str],
        *,
        username: str | None = None,
        password: str | None = None,
        namespace: str = "default",
        cluster: bool = False,
        pool_size: int = 16,
        timeout_ms: int = 2000,
        ca_file: str | Path | None = None,
        allowed_nodes: Sequence[str] = (),
        allowed_node_suffix: str | None = None,
    ) -> Cache:
        """Open a verified connection and discover slot owners in cluster mode."""
        seeds = _endpoints(endpoints, 32)
        if not seeds or type(cluster) is not bool or (not cluster and len(seeds) != 1):
            raise ValidationError("cache requires seeds, or one endpoint in standalone mode")
        if not isinstance(namespace, str) or not _NAMESPACE.fullmatch(namespace):
            raise ValidationError("cache namespace must contain 1–64 characters in [a-z0-9_-]")
        size = _checked_number(pool_size, 1, 1024, "pool_size")
        timeout = _checked_number(timeout_ms, 1, 60_000, "timeout_ms")
        nodes = frozenset(_endpoints(allowed_nodes, 1024)) | frozenset(seeds)
        if any(node.tls != seeds[0].tls for node in nodes):
            raise ValidationError("cache endpoints must use the same transport")
        suffix = None
        if allowed_node_suffix is not None:
            if not seeds[0].tls or not isinstance(allowed_node_suffix, str):
                raise ValidationError("cache node suffix requires TLS and a DNS name")
            suffix = _hostname(allowed_node_suffix)
            if "." not in suffix or suffix[-1].isdigit():
                raise ValidationError("invalid cache node DNS suffix")
        if username is not None and password is None:
            raise ValidationError("cache username requires a password")
        auth = ()
        if password is not None:
            user = () if username is None else (_credential(username, "username"),)
            auth = (b"AUTH", *user, _credential(password, "password"))
        try:
            context = ssl.create_default_context(cafile=ca_file) if seeds[0].tls else None
        except (OSError, TypeError):
            raise ValidationError("invalid cache TLS CA file") from None
        if ca_file is not None and context is None:
            raise ValidationError("cache TLS CA file requires TLS")

        cache = cls(seeds, nodes, suffix, namespace, cluster, timeout, _Pool(size, context, auth))
        deadline = cache._deadline()
        try:
            if cluster:
                cache._refresh(deadline)
            else:
                reply = cache._call(seeds[0], (b"PING",), deadline)
                if isinstance(reply, ErrorReply):
                    raise _reply_error(reply)
                if reply != "PONG":
                    raise ProtocolError("invalid cache PING reply")
            return cache
        except BaseException:
            cache.close()
            raise

    def __init__(self, seeds, allowed, suffix, namespace, cluster, timeout, pool):
        self._seeds = seeds
        self._allowed = allowed
        self._suffix = suffix
        self._ports = frozenset(seed.port for seed in seeds)
        self._prefix = f"nb:{namespace}:".encode("ascii")
        self._cluster = cluster
        self._timeout = timeout / 1000
        self._pool = pool
        self._slots: list[Endpoint | None] = [None] * _SLOT_COUNT
        self._topology_lock = Lock()

    def _deadline(self) -> float:
        return time.monotonic() + self._timeout

    def _key(self, key: str) -> bytes:
        if isinstance(key, str) and 1 <= len(key) <= 250:
            try:
                encoded = key.encode("utf-8")
                if len(encoded) <= 250:
                    return self._prefix + base64.urlsafe_b64encode(encoded).rstrip(b"=")
            except UnicodeEncodeError:
                pass
        raise ValidationError("cache key must contain 1–250 UTF-8 bytes")

    def _authorize(self, endpoint: Endpoint) -> Endpoint:
        if endpoint in self._allowed or (
            self._suffix and endpoint.host.endswith("." + self._suffix) and endpoint.port in self._ports
        ):
            return endpoint
        raise ProtocolError("cache discovery advertised an untrusted endpoint")

    def _address(self, host: bytes, port: int) -> Endpoint:
        if type(port) is not int or not 1 <= port <= 65535:
            raise ProtocolError("invalid cache owner port")
        try:
            name = host.decode("ascii", "strict")
            authority = f"[{name}]:{port}" if ":" in name else f"{name}:{port}"
            return self._authorize(_authority(authority, self._seeds[0].tls, require_port=True))
        except (UnicodeError, ValidationError):
            raise ProtocolError("invalid cache owner address") from None

    def _topology(self, reply) -> list[Endpoint | None]:
        if not isinstance(reply, list) or not 1 <= len(reply) <= _SLOT_COUNT:
            raise ProtocolError("invalid cache slot map")
        slots: list[Endpoint | None] = [None] * _SLOT_COUNT
        for row in reply:
            if not isinstance(row, list) or len(row) < 3:
                raise ProtocolError("invalid cache slot range")
            first, last, owner = row[:3]
            if (type(first) is not int or type(last) is not int or not 0 <= first <= last < _SLOT_COUNT
                or not isinstance(owner, list) or len(owner) < 2 or not isinstance(owner[0], bytes)):
                raise ProtocolError("invalid cache slot range")
            endpoint = self._address(owner[0], owner[1])
            for slot in range(first, last + 1):
                if slots[slot] is not None:
                    raise ProtocolError("overlapping cache slot ranges")
                slots[slot] = endpoint
        return slots

    def _refresh(self, deadline: float) -> None:
        last_error = None
        for seed in self._seeds:
            try:
                reply = self._call(seed, (b"CLUSTER", b"SLOTS"), deadline)
                if isinstance(reply, ErrorReply):
                    raise _reply_error(reply)
                slots = self._topology(reply)
                with self._topology_lock:
                    self._slots = slots
                return
            except (ProtocolError, StorageError, TransportError, OverloadedError) as error:
                last_error = error
        assert last_error is not None
        raise last_error

    def _moved(self, reply: ErrorReply, slot: int) -> Endpoint:
        match = _MOVED.fullmatch(reply.text)
        if not match or int(match[1]) != slot:
            raise ProtocolError("cache MOVED does not identify the requested slot")
        try:
            target = self._authorize(_authority(match[2].decode(), self._seeds[0].tls, require_port=True))
        except ValidationError:
            raise ProtocolError("invalid cache MOVED address") from None
        with self._topology_lock:
            self._slots[slot] = target
        return target

    def _call(self, endpoint: Endpoint, args: tuple[bytes, ...], deadline: float, *, write=False, check=None):
        connection = self._pool.take(endpoint, deadline)
        reusable = False
        submitted = False

        def submit():
            nonlocal submitted
            submitted = True

        try:
            try:
                reply = connection.exchange(args, deadline, submit if write else None)
                if isinstance(reply, ErrorReply):
                    # Preserve a valid negative RESP frame for the routing/error layer.
                    reusable = True
                    return reply
                decoded = reply if check is None else check(reply)
                reusable = True
                return decoded
            except (OSError, UnicodeError, TransportError, ProtocolError) as error:
                if write and submitted:
                    raise UnknownOutcomeError("cache mutation may have executed") from error
                if isinstance(error, (UnicodeError, ProtocolError)):
                    raise ProtocolError("invalid cache reply") from error
                raise TransportError("cache read failed before a complete reply", outcome="not_applied") from error
        finally:
            self._pool.release(endpoint, connection, reusable)

    def _run(self, command: bytes, key: str, *args: bytes, write=False, check=None):
        deadline = self._deadline()
        encoded = self._key(key)
        if self._cluster:
            slot = crc_hqx(encoded, 0) % _SLOT_COUNT
            endpoint = self._slots[slot] or self._seeds[0]
        else:
            slot = -1
            endpoint = self._seeds[0]
        refreshed = False
        for attempt in range(_MAX_REDIRECTS + 1):
            try:
                reply = self._call(endpoint, (command, encoded, *args), deadline, write=write, check=check)
            except TransportError:
                # A write-side transport error can only occur before submission.
                if not self._cluster or refreshed:
                    raise
                refreshed = True
                self._refresh(deadline)
                endpoint = self._slots[slot] or self._seeds[0]
                continue
            if not isinstance(reply, ErrorReply):
                return reply
            if self._cluster and reply.text.startswith(b"MOVED "):
                if attempt == _MAX_REDIRECTS:
                    raise StorageError("cache redirect limit reached")
                endpoint = self._moved(reply, slot)
                continue
            error = _reply_error(reply)
            if write and isinstance(error, ProtocolError):
                raise UnknownOutcomeError("cache mutation received an unrecognized reply") from error
            raise error
        raise StorageError("cache redirect limit reached")

    def get(self, key: str) -> bytes | None:
        """Read exact bytes, or None for a miss."""
        def check(reply):
            if reply is None or isinstance(reply, bytes):
                return reply
            raise ProtocolError("invalid cache GET reply")
        return self._run(b"GET", key, check=check)

    def set(self, key: str, value: bytes, *, ttl_ms: int | None = None,
            when: CacheSetCondition = "always") -> bool:
        """Write bytes, optionally conditioned on the presence of a live key."""
        if not isinstance(value, bytes) or len(value) > MAX_VALUE:
            raise ValidationError("cache value must be bytes and at most 1 MiB")
        if when not in ("always", "absent", "present"):
            raise ValidationError("invalid cache SET condition")
        options = ()
        if ttl_ms is not None:
            ttl = _checked_number(ttl_ms, 1, _MAX_TTL, "ttl_ms")
            options = (b"PX", str(ttl).encode())
        if when != "always":
            options += (b"NX" if when == "absent" else b"XX",)

        def check(reply):
            if reply == "OK":
                return True
            if reply is None and when != "always":
                return False
            raise ProtocolError("invalid cache SET reply")
        return self._run(b"SET", key, value, *options, write=True, check=check)

    def delete(self, key: str) -> bool:
        """Delete a live key."""
        return self._run(b"DEL", key, write=True, check=_boolean)

    def expire(self, key: str, ttl_ms: int) -> bool:
        """Set a relative TTL in milliseconds. Zero deletes the key."""
        ttl = _checked_number(ttl_ms, 0, _MAX_TTL, "ttl_ms")
        return self._run(b"PEXPIRE", key, str(ttl).encode(), write=True, check=_boolean)

    def persist(self, key: str) -> bool:
        """Remove a live key's expiry."""
        return self._run(b"PERSIST", key, write=True, check=_boolean)

    def ttl_ms(self, key: str) -> int:
        """Return remaining whole milliseconds, -1 for persistent or -2 for missing."""
        def check(reply):
            if type(reply) is int and -2 <= reply <= _MAX_TTL:
                return reply
            raise ProtocolError("invalid cache PTTL reply")
        return self._run(b"PTTL", key, check=check)

    def increment(self, key: str, delta: int) -> int:
        """Atomically add a signed 64-bit integer and return the exact result."""
        amount = _checked_number(delta, -(2**63), 2**63 - 1, "delta")

        def check(reply):
            if type(reply) is int:
                return reply
            raise ProtocolError("invalid cache INCRBY reply")
        return self._run(b"INCRBY", key, str(amount).encode(), write=True, check=check)

    def close(self) -> None:
        """Close established and opening connections. Repeated calls succeed."""
        self._pool.close()

    def __enter__(self) -> Cache:
        return self

    def __exit__(self, *_exc) -> None:
        self.close()
