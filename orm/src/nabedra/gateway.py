"""Framework independent trusted HTTP gateway.

Hosts must enforce the 8 MiB body limit while reading their input stream. Checking
an already collected body cannot bound host memory. Use one Gateway per application
worker and close it at shutdown. For integration fixtures only, ``_transport`` and
``_async_transport`` accept HTTPX MockTransport instances; production callers should
leave both unset. For example::

    gateway = Gateway(cluster_url="https://compute.internal", token=private_token,
                      mount_path="/nabedra")
    @gateway.middleware
    def authorize(ctx):
        ctx.principal = authenticated_user_id
        ctx.storage = S3("authorized-bucket", region="eu-west-1", credentials=credentials)
    # Pass raw HTTP values to handle() or await handle_async(), then return the
    # HTTPResponse status, headers and body unchanged from the hosting server.

Middleware and credential providers must cooperate with shutdown. A deadline stops
waiting, not the Python callback thread. Its thread and request capacity stay
charged until it returns. close/aclose return without joining running callbacks;
the interpreter still waits for executor threads on exit. Use callbacks with
bounded I/O deadlines. HTTPS is required except for literal loopback fixture addresses.
"""
from __future__ import annotations
import asyncio
import base64
from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone
import inspect
import ipaddress
import json
import math
import re
import threading
import time
import unicodedata
from urllib.parse import urlsplit

from .aws import AwsCredentials
from . import gateway_protocol as wire

@dataclass(frozen=True)
class HTTPResponse:
    status: int
    headers: tuple[tuple[str, str], ...]
    body: bytes = field(repr=False)

@dataclass(frozen=True, repr=False)
class S3:
    bucket: str
    region: str = field(kw_only=True)
    prefix: str = field(default='nabedra/', kw_only=True)
    credentials: object = field(kw_only=True, repr=False)

    def __post_init__(self):
        wire.require(isinstance(self.bucket, str) and re.fullmatch(r'[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]', self.bucket))
        wire.require('..' not in self.bucket and not self.bucket.startswith(('xn--', 'sthree-', 'amzn-s3-demo-')) and not self.bucket.endswith(('-s3alias', '--ol-s3', '.mrap', '--x-s3', '--table-s3')))
        try:
            ipaddress.ip_address(self.bucket)
        except ValueError:
            pass
        else:
            raise ValueError('Bucket must be a general-purpose S3 bucket')
        wire.require(type(self.region) is str and len(self.region) <= 64 and re.fullmatch(
            r'(?:(?:af|ap|ca|cn|eu|il|me|mx|sa|us)-(?:east|west|north|south|central|northeast|northwest|southeast|southwest)|us-gov-(?:east|west))-[1-9][0-9]*', self.region))
        wire.require(type(self.prefix) is str and len(self.prefix.encode('utf-8')) <= 1024)
        prefix = self.prefix.rstrip('/')
        wire.require(prefix and not any(c in prefix for c in '\\:%?#')
                     and not any(unicodedata.category(c) == 'Cc' for c in prefix))
        wire.require(all(p not in ('', '.', '..') for p in prefix.split('/')))
        wire.require(len((prefix + '/').encode('utf-8')) <= 1024)
        object.__setattr__(self, 'prefix', prefix + '/')
        wire.require(isinstance(self.credentials, AwsCredentials) or callable(self.credentials))

    def __repr__(self):
        return 'S3(<redacted>)'

    def _header(self):
        try:
            c = self.credentials() if callable(self.credentials) else self.credentials
            wire.require(isinstance(c, AwsCredentials))
            wire.require(c.expires_at is None or c.expires_at > datetime.now(timezone.utc))
            value = {'version': 1, 'bucket': self.bucket, 'region': self.region, 'prefix': self.prefix,
                     'credentials': {'access_key_id': c.access_key_id, 'secret_access_key': c.secret_access_key,
                                     'session_token': c.session_token,
                                     'expires_at': c.expires_at.isoformat(timespec='seconds').replace('+00:00', 'Z') if c.expires_at else None}}
            raw = json.dumps(value, ensure_ascii=False, separators=(',', ':'), allow_nan=False).encode('utf-8')
            wire.require(len(raw) <= 16 * 1024)
            header = base64.urlsafe_b64encode(raw).rstrip(b'=').decode('ascii')
            wire.require(len(header) <= 24 * 1024)
            return header
        except Exception:
            raise _Rejected('unauthorized', 'Storage credentials unavailable') from None

@dataclass(frozen=True, repr=False)
class Operation:
    request_id: str
    name: str
    payload: Mapping

@dataclass(frozen=True)
class Receipt:
    run_id: str
    request_id: str

class Headers(Mapping):
    """Read-only case-insensitive header view, retaining duplicate pairs."""
    __slots__ = ('_pairs',)
    def __init__(self, pairs):
        object.__setattr__(self, '_pairs', tuple(pairs))
    def __setattr__(self, name, value):
        raise AttributeError('Headers are read-only')
    def __getitem__(self, key):
        values = self.get_all(key)
        if not values:
            raise KeyError(key)
        return ', '.join(values)
    def get_all(self, key):
        return tuple(v for k, v in self._pairs if k.lower() == key.lower())
    @property
    def pairs(self):
        return self._pairs
    def __iter__(self):
        return iter(dict((k.lower(), None) for k, v in self._pairs))
    def __len__(self):
        return len(set(k.lower() for k, v in self._pairs))
    def __repr__(self):
        return 'Headers(<redacted>)'

class _Rejected(Exception):
    def __init__(self, code, message):
        self.code, self.message = code, message
        super().__init__('Gateway request rejected')

class GatewayContext:
    __slots__ = ('_values', 'principal', 'storage')
    def __init__(self, **values):
        object.__setattr__(self, '_values', wire.freeze(values))
        self.principal = None
        self.storage = None
    def __getattr__(self, key):
        try:
            return self._values[key]
        except KeyError:
            raise AttributeError(key) from None
    def __setattr__(self, key, value):
        if key not in ('principal', 'storage'):
            raise AttributeError('Gateway input is read-only')
        object.__setattr__(self, key, value)
    def reject(self, code, message):
        # Middleware messages can contain secrets. Return a fixed public message.
        if code not in ('unauthorized', 'validation_error', 'not_found', 'overloaded'):
            code = 'unauthorized'
        raise _Rejected(code, 'Request rejected by application middleware')
    def __repr__(self):
        return 'GatewayContext(<redacted>)'

_STATUS = {'validation_error': 400, 'unauthorized': 401, 'not_found': 404,
           'limit_exceeded': 413, 'overloaded': 429, 'storage_unavailable': 503,
           'outcome_unknown': 503}

def _error(request_id, code, message, unknown=False):
    body = json.dumps({'request_id': request_id, 'error': {'code': code, 'message': message,
                      'outcome': 'unknown' if unknown else 'not_applied'}}, separators=(',', ':')).encode()
    return HTTPResponse(_STATUS.get(code, 503), (('content-type', 'application/json'), ('cache-control', 'no-store')), body)

def _selector(value):
    wire.require(type(value) is str and re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,127}', value))
    return value

class Gateway:
    def __init__(self, *, cluster_url, token, mount_path='', max_concurrency=16,
                 timeout=30.0, middleware_timeout=10.0, _transport=None, _async_transport=None):
        try:
            url = urlsplit(cluster_url)
            wire.require(url.scheme in ('https', 'http') and url.hostname and not url.username and not url.password and not url.query and not url.fragment and url.path in ('', '/'))
            url.port
            if url.scheme == 'http':
                wire.require(ipaddress.ip_address(url.hostname).is_loopback)
            wire.require(not any(ord(c) <= 32 or ord(c) >= 127 for c in cluster_url) and '\\' not in cluster_url)
            wire.require(type(token) is str and re.fullmatch(r'[A-Za-z0-9._~+/\-]+=*', token))
            wire.require(type(mount_path) is str and (mount_path in ('', '/') or re.fullmatch(r'(?:/[A-Za-z0-9_-]+)+', mount_path)))
            wire.require(type(max_concurrency) is int and 0 < max_concurrency <= 1024)
            for duration in (timeout, middleware_timeout):
                wire.require(type(duration) in (int, float) and math.isfinite(duration) and duration > 0)
        except (ValueError, TypeError):
            raise ValueError('Invalid gateway configuration') from None
        import httpx
        self._url, self._token = cluster_url.rstrip('/'), token
        self._mount = mount_path.rstrip('/')
        self._timeout, self._middleware_timeout = timeout, middleware_timeout
        limits = httpx.Limits(max_connections=max_concurrency, max_keepalive_connections=max_concurrency)
        self._client = httpx.Client(transport=_transport if _transport is not None else httpx.HTTPTransport(retries=0, limits=limits, trust_env=False), timeout=timeout, limits=limits, trust_env=False, follow_redirects=False)
        self._async_client = httpx.AsyncClient(transport=_async_transport if _async_transport is not None else httpx.AsyncHTTPTransport(retries=0, limits=limits, trust_env=False), timeout=timeout, limits=limits, trust_env=False, follow_redirects=False)
        self._slots = threading.BoundedSemaphore(max_concurrency)
        self._thread_slots = threading.BoundedSemaphore(max_concurrency)
        self._executor = ThreadPoolExecutor(max_workers=max_concurrency, thread_name_prefix='nabedra-gateway')
        self._callbacks = []
        self._closed = False
        self._started = False

    def __repr__(self):
        return 'Gateway(<redacted>)'

    def middleware(self, callback):
        if self._started or self._closed or not callable(callback):
            raise ValueError('Register middleware before serving requests')
        self._callbacks.append(callback)
        return callback

    def _parse(self, method, path, headers, body, correlation):
        wire.require(type(method) is str and type(path) is str and type(body) is bytes and type(headers) is tuple)
        if len(body) > wire.MAX_BODY:
            raise _Rejected('limit_exceeded', 'Request body exceeds 8 MiB')
        wire.require(len(path) <= 1024 and path.startswith('/') and not any(c in path for c in ('?', '#', '%', '\\')))
        wire.require(path.startswith(self._mount + '/'))
        path = path[len(self._mount):]
        size = 0
        names = {}
        for pair in headers:
            wire.require(type(pair) is tuple and len(pair) == 2)
            k, v = pair
            wire.require(type(k) is str and type(v) is str and re.fullmatch(r"[!#$%&'*+.^_`|~0-9A-Za-z-]+", k))
            wire.require(not any(ord(c) < 32 and c != '\t' or ord(c) == 127 for c in v))
            size += len(k.encode('ascii')) + len(v.encode('utf-8')) + 4
            if size > 32 * 1024:
                raise _Rejected('limit_exceeded', 'Request headers exceed 32 KiB')
            key = k.lower()
            names[key] = names.get(key, 0) + 1
            if key.startswith('x-nabedra-') and key != 'x-nabedra-database':
                raise _Rejected('unauthorized', 'Untrusted internal header')
        h = Headers(headers)
        wire.require(all(names.get(k, 0) <= 1 for k in ('authorization', 'x-nabedra-database', 'content-type', 'content-length', 'transfer-encoding')))
        wire.require('transfer-encoding' not in names)
        if 'content-length' in names:
            length = h['content-length']
            wire.require(length.isascii() and length.isdecimal() and int(length) == len(body))
        database = _selector(h['x-nabedra-database']) if 'x-nabedra-database' in names else None
        op = receipt = None
        if method == 'POST' and path in ('/v1/operations', '/v1/requests'):
            wire.require(h.get('content-type', '').split(';')[0].strip().lower() == 'application/json')
            envelope = wire.loads(body)
            fields = {'request_id', 'operation', 'payload'}
            wire.fields(envelope, fields)
            correlation[0] = wire.identity(envelope['request_id'])
            wire.payload(envelope['operation'], envelope['payload'])
            op = Operation(envelope['request_id'], envelope['operation'], wire.freeze(envelope['payload']))
        elif method == 'GET' and path.startswith('/v1/requests/'):
            parts = path.split('/')
            wire.require(len(parts) == 5 and not body)
            receipt = Receipt(wire.identity(parts[3]), wire.identity(parts[4]))
        else:
            raise ValueError('Unsupported route')
        return GatewayContext(method=method, path=path, headers=h, raw_body=body,
                              database=database, operation=op, receipt=receipt)

    def _thread(self, callback, *args):
        if not self._thread_slots.acquire(blocking=False):
            raise _Rejected('overloaded', 'Middleware capacity exhausted')
        def run():
            try:
                return callback(*args)
            finally:
                self._thread_slots.release()
        try:
            future = self._executor.submit(run)
        except BaseException:
            self._thread_slots.release()
            raise
        future.add_done_callback(lambda f: self._thread_slots.release() if f.cancelled() else None)
        return future

    def _storage(self, ctx):
        if not isinstance(ctx.storage, S3) or not isinstance(ctx.principal, str) or not ctx.principal or len(ctx.principal) > 1024:
            raise _Rejected('unauthorized', 'Middleware must authorize identity and storage')
        return ctx.storage._header()

    def _headers(self, storage):
        return {'Authorization': 'Bearer ' + self._token, 'X-Nabedra-Storage': storage,
                'Content-Type': 'application/json', 'Accept': 'application/json', 'Accept-Encoding': 'identity'}

    def _response(self, ctx, status, headers, raw):
        wire.require(headers.get('content-type', '').split(';')[0].strip().lower() == 'application/json')
        wire.require(headers.get('content-encoding', 'identity') == 'identity')
        length = headers.get('content-length')
        wire.require(length is None or length.isascii() and length.isdecimal() and int(length) == len(raw))
        value = wire.loads(raw)
        rid = (ctx.operation or ctx.receipt).request_id
        wire.require(type(value) is dict and (value.get('request_id') in (rid, None) if 400 <= status <= 599 else value.get('request_id') == rid))
        if status >= 400 and status <= 599:
            wire.fields(value, {'request_id', 'error'})
            self._validate_error(value['error'])
            code = value['error']['code']
            wire.require(status == {**_STATUS, 'conflict': 409, 'unsupported_operation': 400, 'internal_error': 500}.get(code, 500))
        elif status == 202:
            wire.fields(value, {'request_id', 'run_id', 'state'})
            wire.identity(value['run_id'])
            wire.require(ctx.path == '/v1/requests' and value['state'] == 'queued')
        elif status == 200:
            if ctx.receipt:
                wire.fields(value, {'request_id', 'run_id', 'state', 'result', 'error'})
                wire.require(value['run_id'] == ctx.receipt.run_id and value['state'] in ('queued', 'running', 'committing', 'reconciling', 'succeeded', 'failed', 'abandoned'))
                state = value['state']
                if value['error'] is not None:
                    self._validate_error(value['error'])
                wire.require((state == 'failed' and value['error'] is not None) or (state == 'succeeded' and value['error'] is None) or (state in ('queued', 'running', 'committing', 'reconciling', 'abandoned') and value['error'] is None and value['result'] is None))
            else:
                wire.require(ctx.path == '/v1/operations')
                wire.fields(value, {'request_id', 'data'})
        else:
            raise ValueError('Invalid upstream status')
        safe = [('content-type', headers['content-type']), ('cache-control', 'no-store')]
        if status == 202:
            safe.append(('location', self._mount + '/v1/requests/' + value['run_id'] + '/' + rid))
        return HTTPResponse(status, tuple(safe), raw)

    @staticmethod
    def _validate_error(value):
        wire.fields(value, {'code', 'message', 'outcome'})
        wire.require(type(value['code']) is str and value['code'] and type(value['message']) is str and value['outcome'] in ('not_applied', 'unknown'))
        wire.require(value['code'] != 'outcome_unknown' or value['outcome'] == 'unknown')

    def _failure(self, ctx, sent, error, request_id=None):
        rid = (ctx.operation or ctx.receipt).request_id if ctx else request_id
        if sent and ctx.operation and ctx.operation.name in wire.MUTATIONS:
            return _error(rid, 'outcome_unknown', 'Compute response unavailable; do not resubmit', True)
        if isinstance(error, _Rejected):
            return _error(rid, error.code, error.message)
        return _error(rid, 'storage_unavailable' if sent else 'validation_error',
                      'Compute response unavailable' if sent else 'Gateway validation or middleware failed')

    def _enter_request(self):
        self._started = True
        if self._closed:
            raise _Rejected('storage_unavailable', 'Gateway is closed')
        if not self._slots.acquire(blocking=False):
            raise _Rejected('overloaded', 'Gateway capacity exhausted')

    def handle(self, *, method: str, path: str, headers: tuple[tuple[str, str], ...], body: bytes) -> HTTPResponse:
        ctx, sent, entered, pending = None, False, False, None
        correlation = [None]
        try:
            self._enter_request()
            entered = True
            ctx = self._parse(method, path, headers, body, correlation)
            if any(inspect.iscoroutinefunction(cb) or inspect.iscoroutinefunction(getattr(cb, '__call__', None)) for cb in self._callbacks):
                raise ValueError('Async middleware requires handle_async')
            deadline = time.monotonic() + self._middleware_timeout
            for cb in self._callbacks:
                pending = self._thread(cb, ctx)
                result = pending.result(timeout=max(0, deadline - time.monotonic()))
                pending = None
                if inspect.isawaitable(result):
                    if inspect.iscoroutine(result):
                        result.close()
                    raise ValueError('Async middleware requires handle_async')
            pending = self._thread(self._storage, ctx)
            storage = pending.result(timeout=max(0, deadline - time.monotonic()))
            pending = None
            wire.require(not self._closed)
            sent = True
            pending = self._thread(self._exchange, ctx, storage, time.monotonic() + self._timeout)
            result = pending.result(timeout=self._timeout)
            pending = None
            return result
        except Exception as error:
            return self._failure(ctx, sent, error, correlation[0])
        finally:
            if entered:
                if pending is not None and not pending.done():
                    pending.add_done_callback(lambda _: self._slots.release())
                else:
                    self._slots.release()

    def _exchange(self, ctx, storage, deadline):
        wire.require(not self._closed and time.monotonic() < deadline)
        with self._client.stream(ctx.method, self._url + ctx.path, headers=self._headers(storage),
                                 content=ctx.raw_body, timeout=max(.001, deadline - time.monotonic())) as response:
            wire.require(response.headers.get('content-encoding', 'identity') == 'identity')
            raw = bytearray()
            for chunk in response.iter_bytes(chunk_size=65536):
                wire.require(len(raw) + len(chunk) <= wire.MAX_BODY and time.monotonic() < deadline)
                raw.extend(chunk)
            return self._response(ctx, response.status_code, response.headers, bytes(raw))

    async def handle_async(self, *, method: str, path: str, headers: tuple[tuple[str, str], ...], body: bytes) -> HTTPResponse:
        ctx, sent, entered, pending = None, False, False, None
        # Only admitted parsing may inspect the body, including for error IDs.
        correlation = [None]
        try:
            self._enter_request()
            entered = True
            pending = self._thread(self._parse, method, path, headers, body, correlation)
            ctx = await asyncio.wrap_future(pending)
            pending = None
            async with asyncio.timeout(self._middleware_timeout):
                for cb in self._callbacks:
                    if inspect.iscoroutinefunction(cb) or inspect.iscoroutinefunction(getattr(cb, '__call__', None)):
                        await cb(ctx)
                    else:
                        pending = self._thread(cb, ctx)
                        result = await asyncio.wrap_future(pending)
                        pending = None
                        if inspect.isawaitable(result):
                            await result
                pending = self._thread(self._storage, ctx)
                storage = await asyncio.wrap_future(pending)
                pending = None
            wire.require(not self._closed)
            sent = True
            async with asyncio.timeout(self._timeout):
                async with self._async_client.stream(ctx.method, self._url + ctx.path, headers=self._headers(storage), content=ctx.raw_body) as response:
                    wire.require(response.headers.get("content-encoding", "identity") == "identity")
                    raw = bytearray()
                    async for chunk in response.aiter_bytes(chunk_size=65536):
                        wire.require(len(raw) + len(chunk) <= wire.MAX_BODY)
                        raw.extend(chunk)
                    return self._response(ctx, response.status_code, response.headers, bytes(raw))
        except Exception as error:
            return self._failure(ctx, sent, error, correlation[0])
        finally:
            if entered:
                if pending is not None and not pending.done():
                    pending.add_done_callback(lambda _: self._slots.release())
                else:
                    self._slots.release()

    def close(self):
        self._closed = True
        self._executor.shutdown(wait=False, cancel_futures=True)
        self._client.close()
        # Async client is lazy and has no connections until handle_async is used.
        if not self._async_client.is_closed:
            try:
                asyncio.get_running_loop()
            except RuntimeError:
                asyncio.run(self._async_client.aclose())
            else:
                raise RuntimeError('Use await aclose() in async applications')

    async def aclose(self):
        self._closed = True
        self._executor.shutdown(wait=False, cancel_futures=True)
        self._client.close()
        async with asyncio.timeout(self._timeout):
            await self._async_client.aclose()

    def __enter__(self):
        wire.require(not self._closed)
        return self
    def __exit__(self, *args):
        self.close()
    async def __aenter__(self):
        wire.require(not self._closed)
        return self
    async def __aexit__(self, *args):
        await self.aclose()
