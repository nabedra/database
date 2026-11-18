"""Private synchronous HTTP transport and public Database creation/discovery."""
from __future__ import annotations

from collections.abc import Callable
from http.client import HTTPException, IncompleteRead
import json
import math
import os
import re
import ssl
from typing import TYPE_CHECKING, Literal, TypeVar
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit, urlunsplit
from urllib.request import HTTPRedirectHandler, HTTPSHandler, Request, build_opener
import uuid
import time
from dataclasses import dataclass, field

from .errors import (
    AuthenticationError, ConflictError, LimitExceededError, NabedraError,
    NotFoundError, OverloadedError, ProtocolError, StorageError, TransportError,
    UnknownOutcomeError, ValidationError,
)
from .validation import (
    Schema, schema_to_wire, validate_format, validate_identifier, validate_schema,
)

if TYPE_CHECKING:
    from .table import Table

_MAX_BODY = 8 * 1024 * 1024
_MUTATIONS = frozenset({
    'table.create', 'table.drop', 'table.insert', 'table.insert_many',
    'table.update', 'table.upsert', 'table.delete',
})
_READS = frozenset({
    'table.describe', 'table.list', 'table.get',
    'query.collect', 'query.count', 'query.explain',
})
_ERROR_STATUS = {
    'validation_error': 400, 'unsupported_operation': 400, 'unauthorized': 401,
    'not_found': 404, 'conflict': 409, 'limit_exceeded': 413, 'overloaded': 429,
    'storage_unavailable': 503, 'outcome_unknown': 503,
}
_ERROR_TYPES = {
    'validation_error': ValidationError, 'unauthorized': AuthenticationError,
    'not_found': NotFoundError, 'conflict': ConflictError,
    'limit_exceeded': LimitExceededError, 'overloaded': OverloadedError,
    'storage_unavailable': StorageError,
}
_T = TypeVar('_T')


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError('Duplicate JSON field')
        result[key] = value
    return result


def _invalid_constant(value):
    raise ValueError('Nonfinite JSON number')


def _finite_float(value):
    result = float(value)
    if not math.isfinite(result):
        raise ValueError('Nonfinite JSON number')
    return result


class _Transport:
    """Validate connection configuration once; construction sends no requests.

    _Transport(api_url: str, token: str, *, timeout: float = 30.0,
               ca_file: str | None = None)

    Each instance owns its opener and verified TLS context. Timeout applies to
    blocking socket I/O, not cancellation or a server transaction deadline.
    """

    def __init__(self, api_url: str, token: str | None, *, timeout: float = 30.0,
                 ca_file: str | None = None, database: str | None = None) -> None:
        try:
            if (not isinstance(api_url, str) or not api_url
                    or any(ord(c) <= 32 or ord(c) >= 127 for c in api_url)
                    or '?' in api_url or '#' in api_url or '\\' in api_url):
                raise ValueError
            url = urlsplit(api_url)
            if (url.scheme not in {'http', 'https'} or not url.hostname
                    or url.username is not None or url.password is not None):
                raise ValueError
            # Accessing port also checks malformed/out-of-range ports.
            url.port
        except (ValueError, TypeError):
            raise ValidationError('API URL must be absolute HTTP(S), without credentials, query or fragment') from None
        if token is not None and (not isinstance(token, str) or re.fullmatch(r'[A-Za-z0-9._~+/\-]+=*', token) is None):
            raise ValidationError('Token must be a nonempty safe bearer token')
        if type(timeout) not in (int, float):
            raise ValidationError('Timeout must be positive and finite')
        try:
            checked_timeout = float(timeout)
        except OverflowError:
            raise ValidationError('Timeout must be positive and finite') from None
        if not math.isfinite(checked_timeout) or checked_timeout <= 0:
            raise ValidationError('Timeout must be positive and finite')
        if ca_file is not None and (not isinstance(ca_file, str) or not ca_file or '\x00' in ca_file):
            raise ValidationError('CA file must be a nonempty path string')
        try:
            context = ssl.create_default_context()
            if ca_file is not None:
                context.load_verify_locations(cafile=ca_file)
        except (OSError, ValueError):
            raise ValidationError('Could not load TLS trust configuration') from None
        if database is not None and (not isinstance(database, str) or re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,127}', database) is None):
            raise ValidationError('Invalid database selector')
        self._database_headers = {'X-Nabedra-Database': database} if database is not None else {}
        self._url = urlunsplit((url.scheme, url.netloc, url.path.rstrip('/') + '/v1/operations', '', ''))
        self._token = token
        self._timeout = checked_timeout
        self._tls_context = context
        self._opener = build_opener(_NoRedirect(), HTTPSHandler(context=context))

    def _require_token(self) -> str:
        if self._token is None:
            raise AuthenticationError('Set NABEDRA_TOKEN or pass token before making a request')
        return self._token

    def request(self, operation: str, payload: dict, *,
                decode: Callable[[object], _T] | None = None) -> _T | object:
        """POST one v0 operation and return data, or the exact decode(data) result.

        Decoders must validate server data and raise ValidationError,
        LimitExceededError or ProtocolError for invalid results. Those failures
        become ProtocolError for reads and UnknownOutcomeError for mutations.
        Other decoder exceptions propagate as programming errors. Never retry;
        request IDs identify one queued operation and are never automatically resubmitted.
        All raised transport/server failures retain the client's request ID and
        HTTP status when available. Successful results are returned unwrapped.
        """
        if not isinstance(operation, str) or operation not in _READS | _MUTATIONS:
            raise ValidationError('Unsupported v0 operation')
        if not isinstance(payload, dict):
            raise ValidationError('Operation payload must be a dictionary')
        if decode is not None and not callable(decode):
            raise ValidationError('Result decoder must be callable')
        request_id = str(uuid.uuid4())
        try:
            body = json.dumps({'request_id': request_id, 'operation': operation, 'payload': payload},
                              ensure_ascii=False, allow_nan=False, separators=(',', ':')).encode('utf-8')
        except (ValueError, TypeError, UnicodeError, RecursionError):
            raise ValidationError('Operation payload must encode as finite UTF-8 JSON') from None
        if len(body) > _MAX_BODY:
            raise LimitExceededError('Encoded request exceeds 8 MiB')
        request = Request(self._url, data=body, method='POST', headers={
            **self._database_headers,
            'Content-Type': 'application/json', 'Accept': 'application/json',
            'Authorization': 'Bearer ' + self._require_token(),
        })
        mutation = operation in _MUTATIONS
        status = None

        def failure(message, *, network=False):
            error_type = UnknownOutcomeError if mutation else (TransportError if network else ProtocolError)
            return error_type(message, request_id=request_id, status_code=status)

        try:
            try:
                response = self._opener.open(request, timeout=self._timeout)
            except HTTPError as error:
                # HTTPError is also a response stream and must always be closed.
                response = error
            try:
                status = response.status
                raw = response.read(_MAX_BODY + 1)
                content_type = response.headers.get('Content-Type', '').split(';', 1)[0].strip().lower()
                length = response.headers.get('Content-Length')
            finally:
                response.close()
        except (OSError, URLError, IncompleteRead):
            # RemoteDisconnected is also an OSError; a lost response is not
            # evidence that the peer sent malformed HTTP.
            raise failure('Response lost during HTTP I/O', network=True) from None
        except HTTPException:
            raise failure('Malformed HTTP response') from None
        if len(raw) > _MAX_BODY:
            raise failure('Response exceeds 8 MiB')
        if length is not None:
            try:
                if not length.isascii() or not length.isdecimal():
                    raise ValueError
                expected_length = int(length)
            except ValueError:
                raise failure('Invalid response Content-Length') from None
            if expected_length != len(raw):
                raise failure('Incomplete response body', network=True)
        if content_type != 'application/json':
            raise failure('Response is not application/json')
        try:
            envelope = json.loads(raw.decode('utf-8'), object_pairs_hook=_unique_object,
                                  parse_constant=_invalid_constant, parse_float=_finite_float)
        except (ValueError, UnicodeError, RecursionError):
            raise failure('Malformed response JSON') from None
        if not isinstance(envelope, dict):
            raise failure('Invalid response envelope')
        if status == 200:
            if set(envelope) != {'request_id', 'data'} or envelope['request_id'] != request_id:
                raise failure('Invalid success envelope or request ID')
            data = envelope['data']
            if decode is None:
                return data
            try:
                return decode(data)
            except (ValidationError, LimitExceededError, ProtocolError):
                raise failure('Invalid operation result') from None
        if type(status) is not int or not 400 <= status <= 599:
            raise failure('Invalid response status')
        if set(envelope) != {'request_id', 'error'}:
            raise failure('Invalid error envelope')
        if envelope['request_id'] is not None and envelope['request_id'] != request_id:
            raise failure('Mismatched error request ID')
        error = envelope['error']
        if (not isinstance(error, dict) or set(error) != {'code', 'message', 'outcome'}
                or not isinstance(error['code'], str) or not error['code']
                or not isinstance(error['message'], str)
                or error['outcome'] not in ('not_applied', 'unknown')):
            raise failure('Invalid error details')
        code, outcome = error['code'], error['outcome']
        if _ERROR_STATUS.get(code, 500) != status or (code == 'outcome_unknown' and outcome != 'unknown'):
            raise failure('Error code/status/outcome mismatch')
        error_type = UnknownOutcomeError if outcome == 'unknown' else _ERROR_TYPES.get(code, NabedraError)
        raise error_type(error['message'], code=code, request_id=request_id,
                         status_code=status, outcome=outcome)

    def _receipt_exchange(self, method: str, suffix: str, body: bytes | None,
                          request_id: str, *, mutation: bool, timeout: float | None = None) -> tuple[int, dict]:
        request = Request(self._url.removesuffix('/v1/operations') + suffix,
                          data=body, method=method, headers={
                              **self._database_headers,
                              'Content-Type': 'application/json', 'Accept': 'application/json',
                              'Authorization': 'Bearer ' + self._require_token(),
                          })
        status = None
        def failure(message, *, network=False):
            kind = UnknownOutcomeError if mutation else (TransportError if network else ProtocolError)
            return kind(message, request_id=request_id, status_code=status)
        try:
            try:
                response = self._opener.open(request, timeout=self._timeout if timeout is None else min(timeout, self._timeout))
            except HTTPError as error:
                response = error
            try:
                status = response.status
                raw = response.read(_MAX_BODY + 1)
                content_type = response.headers.get('Content-Type', '').split(';', 1)[0].strip().lower()
                length = response.headers.get('Content-Length')
            finally:
                response.close()
        except (OSError, URLError, IncompleteRead):
            raise failure('Response lost during HTTP I/O', network=True) from None
        except HTTPException:
            raise failure('Malformed HTTP response') from None
        if len(raw) > _MAX_BODY or content_type != 'application/json':
            raise failure('Invalid receipt response body')
        if length is not None and (not length.isascii() or not length.isdecimal() or int(length) != len(raw)):
            raise failure('Incomplete receipt response body', network=True)
        try:
            value = json.loads(raw.decode('utf-8'), object_pairs_hook=_unique_object,
                               parse_constant=_invalid_constant, parse_float=_finite_float)
        except (ValueError, UnicodeError, RecursionError):
            raise failure('Malformed receipt response JSON') from None
        if not isinstance(value, dict):
            raise failure('Invalid receipt envelope')
        if status in (200, 202):
            return status, value
        if (type(status) is not int or not 400 <= status <= 599
                or set(value) != {'request_id', 'error'}
                or value['request_id'] not in (None, request_id)):
            raise failure('Invalid receipt error envelope')
        error = value['error']
        if (not isinstance(error, dict) or set(error) != {'code', 'message', 'outcome'}
                or not isinstance(error['code'], str) or not isinstance(error['message'], str)
                or error['outcome'] not in ('not_applied', 'unknown')
                or _ERROR_STATUS.get(error['code'], 500) != status
                or error['code'] == 'outcome_unknown' and error['outcome'] != 'unknown'):
            raise failure('Invalid receipt error details')
        kind = UnknownOutcomeError if error['outcome'] == 'unknown' else _ERROR_TYPES.get(error['code'], NabedraError)
        raise kind(error['message'], code=error['code'], outcome=error['outcome'],
                   request_id=request_id, status_code=status)

    def submit(self, operation: str, payload: dict, *, request_id: str | None = None) -> RequestReceipt:
        if not isinstance(operation, str) or operation not in _READS | _MUTATIONS:
            raise ValidationError('Unsupported operation')
        if not isinstance(payload, dict):
            raise ValidationError('Operation payload must be a dictionary')
        request_id = _receipt_uuid(request_id) if request_id is not None else str(uuid.uuid4())
        try:
            body = json.dumps({'request_id': request_id, 'operation': operation, 'payload': payload},
                              ensure_ascii=False, allow_nan=False, separators=(',', ':')).encode('utf-8')
        except (ValueError, TypeError, UnicodeError, RecursionError):
            raise ValidationError('Operation payload must encode as finite UTF-8 JSON') from None
        if len(body) > _MAX_BODY:
            raise LimitExceededError('Encoded request exceeds 8 MiB')
        status, value = self._receipt_exchange('POST', '/v1/requests', body, request_id,
                                               mutation=operation in _MUTATIONS)
        if status != 202 or set(value) != {'request_id', 'run_id', 'state'} or value['request_id'] != request_id or value['state'] != 'queued':
            kind = UnknownOutcomeError if operation in _MUTATIONS else ProtocolError
            raise kind('Invalid acceptance receipt', request_id=request_id, status_code=status)
        try:
            run_id = _receipt_uuid(value['run_id'], protocol=True)
        except ProtocolError:
            kind = UnknownOutcomeError if operation in _MUTATIONS else ProtocolError
            raise kind('Invalid acceptance run identity', request_id=request_id, status_code=status) from None
        return RequestReceipt(request_id, run_id, 'queued', None, None, self)

    def receipt(self, run_id: str, request_id: str, *, timeout: float | None = None) -> RequestReceipt:
        run_id, request_id = _receipt_uuid(run_id), _receipt_uuid(request_id)
        status, value = self._receipt_exchange('GET', f'/v1/requests/{run_id}/{request_id}', None,
                                               request_id, mutation=False, timeout=timeout)
        if (status != 200 or set(value) != {'request_id', 'run_id', 'state', 'result', 'error'}
                or value['request_id'] != request_id or value['run_id'] != run_id
                or value['state'] not in {'queued', 'running', 'committing', 'reconciling', 'succeeded', 'failed', 'abandoned'}):
            raise ProtocolError('Invalid request receipt', request_id=request_id, status_code=status)
        error = value['error']
        if error is not None and (not isinstance(error, dict) or set(error) != {'code', 'message', 'outcome'}
                                  or not isinstance(error['code'], str) or not isinstance(error['message'], str)
                                  or error['outcome'] not in ('not_applied', 'unknown')):
            raise ProtocolError('Invalid receipt error', request_id=request_id, status_code=status)
        if (value['state'] == 'failed' and error is None
                or value['state'] in {'queued', 'running', 'committing', 'reconciling', 'abandoned'}
                and (value['result'] is not None or error is not None)
                or value['state'] == 'succeeded' and error is not None):
            raise ProtocolError('Invalid receipt state', request_id=request_id, status_code=status)
        return RequestReceipt(request_id, run_id, value['state'], value['result'], error, self)


def _receipt_uuid(value, *, protocol=False):
    kind = ProtocolError if protocol else ValidationError
    if not isinstance(value, str):
        raise kind('Receipt identity must be a UUID')
    try:
        parsed = uuid.UUID(value)
    except ValueError:
        raise kind('Receipt identity must be a UUID') from None
    if str(parsed) != value:
        raise kind('Receipt identity must be a canonical UUID')
    return value


@dataclass(frozen=True)
class RequestReceipt:
    """A recoverable queue identity. Polling never submits an operation again."""
    request_id: str
    run_id: str
    state: str
    result: object
    error: dict | None
    _transport: _Transport = field(repr=False, compare=False)

    @property
    def terminal(self) -> bool:
        return self.state in {'succeeded', 'failed', 'abandoned'}

    def poll(self) -> RequestReceipt:
        return self._transport.receipt(self.run_id, self.request_id)

    def wait(self, *, timeout: float = 30.0, poll_interval: float = 0.05) -> RequestReceipt:
        if type(timeout) not in (int, float) or type(poll_interval) not in (int, float):
            raise ValidationError('Receipt wait durations must be positive and finite')
        try:
            timeout, poll_interval = float(timeout), float(poll_interval)
        except OverflowError:
            raise ValidationError('Receipt wait durations must be positive and finite') from None
        if not math.isfinite(timeout) or timeout <= 0 or not math.isfinite(poll_interval) or poll_interval <= 0:
            raise ValidationError('Receipt wait durations must be positive and finite')
        deadline = time.monotonic() + timeout
        receipt = self
        while not receipt.terminal:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TransportError('Receipt wait timed out; work remains queued', request_id=self.request_id)
            time.sleep(min(poll_interval, remaining))
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TransportError('Receipt wait timed out; work remains queued', request_id=self.request_id)
            receipt = self._transport.receipt(self.run_id, self.request_id, timeout=remaining)
        return receipt


class Database:
    """Synchronous table operations through the server API.

    Mode records the workload preference. Both modes currently use the same
    execution engine. Construction performs no network I/O. Authentication uses
    an explicit token or NABEDRA_TOKEN and is required when an operation starts.
    """

    def __init__(self, url: str | None = None, token: str | None = None, *,
                 mode: Literal['olap', 'oltp'] = 'oltp', timeout: float = 30.0,
                 ca_file: str | None = None, database: str | None = None,
                 api_url: str | None = None) -> None:
        if not isinstance(mode, str) or mode not in ('olap', 'oltp'):
            raise ValidationError('Mode must be olap or oltp')
        if api_url is not None:
            if url is not None:
                raise ValidationError('Supply url only once')
            url = api_url
        if token is None:
            token = os.environ.get('NABEDRA_TOKEN')
        self._transport = _Transport(url, token, timeout=timeout, ca_file=ca_file, database=database)
        self._mode = mode

    @property
    def mode(self) -> Literal['olap', 'oltp']:
        """Requested workload preference; both modes share the current engine."""
        return self._mode

    def submit(self, operation: str, payload: dict, *, request_id: str | None = None) -> RequestReceipt:
        """Accept one operation and return its stable queue identity."""
        return self._transport.submit(operation, payload, request_id=request_id)

    def receipt(self, run_id: str, request_id: str) -> RequestReceipt:
        """Reconnect to an accepted request using the same database credentials."""
        return self._transport.receipt(run_id, request_id)

    def create_table(self, name: str, schema: Schema, key: str, *,
                     format: Literal['parquet'] = 'parquet') -> Table:
        from .table import _table_from_descriptor

        name = validate_identifier(name)
        key = validate_identifier(key)
        schema = validate_schema(schema, key=key)
        format = validate_format(format)
        payload = {'name': name, 'schema': schema_to_wire(schema), 'key': key, 'format': format}

        def descriptor_result(data: object) -> Table:
            table = _table_from_descriptor(self._transport, data)
            if (table.name != name or table.schema != schema
                    or table.key != key or table.format != format):
                raise ProtocolError('Created descriptor does not match submitted metadata')
            return table

        return self._transport.request('table.create', payload, decode=descriptor_result)

    def table(self, name: str) -> Table:
        from .table import _table_from_descriptor

        name = validate_identifier(name)

        def descriptor_result(data: object) -> Table:
            table = _table_from_descriptor(self._transport, data)
            if table.name != name:
                raise ProtocolError('Described table name does not match requested name')
            return table

        return self._transport.request('table.describe', {'name': name}, decode=descriptor_result)

    def tables(self) -> list[Table]:
        from .table import _table_from_descriptor

        def list_result(data: object) -> list[Table]:
            if type(data) is not list:
                raise ProtocolError('Expected a table descriptor list')
            tables = [_table_from_descriptor(self._transport, item) for item in data]
            names = [table.name for table in tables]
            ids = [table.id.lower() for table in tables]
            if (names != sorted(names) or len(set(names)) != len(names)
                    or len(set(ids)) != len(ids)):
                raise ProtocolError('Table list must have sorted unique names and unique IDs')
            return tables

        return self._transport.request('table.list', {}, decode=list_result)


# Convenience alias within the nabedra package.
DB = Database
