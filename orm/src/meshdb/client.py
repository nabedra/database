"""Private synchronous HTTP transport and public DB creation/discovery."""
from __future__ import annotations

from collections.abc import Callable
from http.client import HTTPException, IncompleteRead
import json
import math
import re
import ssl
from typing import TYPE_CHECKING, Literal, TypeVar
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit, urlunsplit
from urllib.request import HTTPRedirectHandler, HTTPSHandler, Request, build_opener
import uuid

from .errors import (
    AuthenticationError, ConflictError, LimitExceededError, MeshDBError,
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

    def __init__(self, api_url: str, token: str, *, timeout: float = 30.0,
                 ca_file: str | None = None) -> None:
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
        if not isinstance(token, str) or re.fullmatch(r'[A-Za-z0-9._~+/\-]+=*', token) is None:
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
        self._url = urlunsplit((url.scheme, url.netloc, url.path.rstrip('/') + '/v1/operations', '', ''))
        self._token = token
        self._timeout = checked_timeout
        self._opener = build_opener(_NoRedirect(), HTTPSHandler(context=context))

    def request(self, operation: str, payload: dict, *,
                decode: Callable[[object], _T] | None = None) -> _T | object:
        """POST one v0 operation and return data, or the exact decode(data) result.

        Decoders must validate server data and raise ValidationError,
        LimitExceededError or ProtocolError for invalid results. Those failures
        become ProtocolError for reads and UnknownOutcomeError for mutations.
        Other decoder exceptions propagate as programming errors. Never retry;
        request IDs are diagnostic and provide no replay/deduplication guarantee.
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
            'Content-Type': 'application/json', 'Accept': 'application/json',
            'Authorization': 'Bearer ' + self._token,
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
        error_type = UnknownOutcomeError if outcome == 'unknown' else _ERROR_TYPES.get(code, MeshDBError)
        raise error_type(error['message'], code=code, request_id=request_id,
                         status_code=status, outcome=outcome)


class DB:
    """Synchronous table creation and discovery through the server API."""

    def __init__(self, api_url: str, token: str, *, timeout: float = 30.0,
                 ca_file: str | None = None) -> None:
        self._transport = _Transport(api_url, token, timeout=timeout, ca_file=ca_file)

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
