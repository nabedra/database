"""HTTP/1.1 connection and bounded response framing for one stream handle."""
from __future__ import annotations

from http.client import HTTPConnection, HTTPException, HTTPResponse, HTTPSConnection, IncompleteRead
import json
import time
from typing import Never
from urllib.parse import urlsplit

from .client import _ERROR_STATUS, _ERROR_TYPES, _Transport
from .client import _finite_float, _invalid_constant, _unique_object
from .errors import (
    LimitExceededError, NabedraError, OverloadedError, ProtocolError,
    StreamAbandonedError, StreamExpiredError, StreamUnsupportedError,
    TransportError, UnknownOutcomeError, ValidationError,
)

_REQUEST_BYTES = 128 * 1024
_CONTROL_BYTES = 64 * 1024
_ATTEMPTS = 3
_CLEANUP_TIMEOUT = 1.0
_MAX_RETRY_AFTER = 1.0
_ERROR_CODES = {
    **_ERROR_STATUS,
    'stream_expired': 410,
    'stream_abandoned': 410,
}
_STREAM_ERRORS = {
    **_ERROR_TYPES,
    'unsupported_operation': StreamUnsupportedError,
    'stream_expired': StreamExpiredError,
    'stream_abandoned': StreamAbandonedError,
}


def _json_object(raw: bytes) -> dict:
    try:
        value = json.loads(raw.decode('utf-8'), object_pairs_hook=_unique_object,
                           parse_constant=_invalid_constant, parse_float=_finite_float)
    except (ValueError, UnicodeError, RecursionError):
        raise ProtocolError('Malformed stream JSON') from None
    if type(value) is not dict:
        raise ProtocolError('Expected a stream JSON object')
    return value


class _StreamResponse(HTTPResponse):
    def _read_and_discard_trailer(self) -> None:
        # The stdlib accepts a disconnect before the terminating CRLF. Keep that
        # case retryable, even if the payload before it was independently valid.
        remaining = 8192
        while remaining > 0:
            line = self.fp.readline(remaining + 1)
            if len(line) > remaining:
                break
            if not line.endswith(b'\r\n'):
                raise IncompleteRead(b'')
            if line == b'\r\n':
                return
            remaining -= len(line)
        raise HTTPException('Stream response trailers exceed 8 KiB')


class _StreamChannel:
    def __init__(self, transport: _Transport) -> None:
        url = urlsplit(transport._url)
        self._path = url.path.removesuffix('/v1/operations')
        self._timeout = transport._timeout
        self._headers = {
            **transport._database_headers,
            'Authorization': 'Bearer ' + transport._require_token(),
            'Accept': 'application/json, application/vnd.apache.arrow.stream',
            'Accept-Encoding': 'identity',
            'Cache-Control': 'no-store',
        }
        if url.scheme == 'https':
            self._connection = HTTPSConnection(
                url.hostname, url.port, timeout=self._timeout, context=transport._tls_context,
            )
        else:
            self._connection = HTTPConnection(url.hostname, url.port, timeout=self._timeout)
        self._connection.response_class = _StreamResponse

    def close(self) -> None:
        self._connection.close()

    def exchange(
        self,
        method: str,
        route: str,
        *,
        expected: tuple[int, ...],
        value: dict | None = None,
        request_id: str | None = None,
        max_bytes: int = _CONTROL_BYTES,
        capabilities: bool = False,
        cleanup: bool = False,
    ) -> tuple[int, str, bytes]:
        body = None
        headers = self._headers
        if value is not None:
            try:
                body = json.dumps(value, ensure_ascii=False, allow_nan=False,
                                  separators=(',', ':')).encode('utf-8')
            except (ValueError, TypeError, UnicodeError, RecursionError):
                raise ValidationError('Stream request must encode as finite UTF-8 JSON') from None
            if len(body) > _REQUEST_BYTES:
                raise LimitExceededError('Encoded stream request exceeds 128 KiB')
            headers = {**headers, 'Content-Type': 'application/json'}
        attempts = 1 if cleanup else _ATTEMPTS
        timeout = min(self._timeout, _CLEANUP_TIMEOUT) if cleanup else self._timeout
        for attempt in range(attempts):
            retry_after = 0.0
            try:
                status, content_type, hint, raw = self._exchange(
                    method, route, body, headers, timeout, expected, max_bytes, request_id,
                )
                if status in expected:
                    return status, content_type, raw
                if capabilities and status == 404 and not _has_error_envelope(content_type, raw):
                    raise StreamUnsupportedError('Server does not expose OLAP streaming capabilities',
                                                 status_code=status)
                retry_after = _retry_delay(hint)
                _raise_response_error(status, content_type, raw, request_id)
            except (TransportError, OverloadedError):
                if attempt + 1 == attempts:
                    raise
                time.sleep(max(retry_after, 0.05 * 2**attempt))
        raise AssertionError('Stream exchange requires at least one attempt')

    def _exchange(
        self, method: str, route: str, body: bytes | None, headers: dict[str, str],
        timeout: float, expected: tuple[int, ...], max_bytes: int, request_id: str | None,
    ) -> tuple[int, str, str | None, bytes]:
        connection = self._connection
        connection.timeout = timeout
        status = None
        try:
            if connection.sock is not None:
                connection.sock.settimeout(timeout)
            connection.request(method, self._path + route, body=body, headers=headers)
            with connection.getresponse() as response:
                status = response.status
                content_types = response.headers.get_all('Content-Type', [])
                if len(content_types) > 1:
                    raise ProtocolError('Duplicate stream response Content-Type')
                content_type = response.headers.get_content_type() if content_types else ''
                bound = max_bytes if status in expected else _CONTROL_BYTES
                raw = _read_body(response, bound, require_frame=status in expected)
                return status, content_type, response.getheader('Retry-After'), raw
        except (OSError, IncompleteRead):
            self.close()
            raise TransportError('Lost stream response during HTTP I/O',
                                 request_id=request_id, status_code=status) from None
        except HTTPException:
            self.close()
            raise ProtocolError('Malformed stream HTTP response',
                                request_id=request_id, status_code=status) from None
        except ProtocolError:
            self.close()
            raise


def _read_body(response: HTTPResponse, max_bytes: int, *, require_frame: bool) -> bytes:
    lengths = response.headers.get_all('Content-Length', [])
    transfers = response.headers.get_all('Transfer-Encoding', [])
    encoding = response.headers.get_all('Content-Encoding', [])
    if (len(lengths) > 1 or (transfers and (lengths or len(transfers) != 1
                                             or transfers[0].strip().lower() != 'chunked'))
            or (encoding and (len(encoding) != 1 or encoding[0].strip().lower() != 'identity'))):
        raise ProtocolError('Invalid stream response framing')
    length = None
    if lengths:
        value = lengths[0]
        if not value.isascii() or not value.isdecimal() or len(value) > 20:
            raise ProtocolError('Invalid stream response Content-Length')
        length = int(value)
    if response.status == 204:
        if length not in (None, 0) or transfers:
            raise ProtocolError('A stream EOF response must have an empty body')
        return response.read()
    if length is not None:
        if length > max_bytes:
            raise ProtocolError('Stream response exceeds its byte limit')
        return response.read()
    if require_frame and not transfers:
        raise ProtocolError('Stream response requires a Content-Length or chunk framing')
    raw = response.read(max_bytes + 1)
    if len(raw) > max_bytes:
        raise ProtocolError('Stream response exceeds its byte limit')
    return raw


def _retry_delay(hint: str | None) -> float:
    if hint and hint.isascii() and hint.isdecimal():
        return min(float(hint), _MAX_RETRY_AFTER)
    return 0.0


def _has_error_envelope(content_type: str, raw: bytes) -> bool:
    if content_type != 'application/json':
        return False
    try:
        return set(_json_object(raw)) == {'request_id', 'error'}
    except ProtocolError:
        return False


def _raise_response_error(status: int, content_type: str, raw: bytes, request_id: str | None) -> Never:
    def malformed(message: str) -> ProtocolError:
        return ProtocolError(message, request_id=request_id, status_code=status)

    if not 400 <= status <= 599 or content_type != 'application/json':
        raise malformed('Unexpected stream response status or content type')
    try:
        envelope = _json_object(raw)
    except ProtocolError as error:
        raise malformed(str(error)) from None
    if set(envelope) != {'request_id', 'error'} or envelope['request_id'] not in (None, request_id):
        raise malformed('Invalid stream error envelope')
    error = envelope['error']
    if (type(error) is not dict or set(error) != {'code', 'message', 'outcome'}
            or type(error['code']) is not str or not error['code']
            or type(error['message']) is not str
            or error['outcome'] not in ('not_applied', 'unknown')):
        raise malformed('Invalid stream error details')
    code, outcome = error['code'], error['outcome']
    if _ERROR_CODES.get(code, 500) != status or (code == 'outcome_unknown' and outcome != 'unknown'):
        raise malformed('Stream error code/status/outcome mismatch')
    if code in ('stream_expired', 'stream_abandoned') and outcome != 'not_applied':
        raise malformed('Invalid stream lease error outcome')
    kind = UnknownOutcomeError if outcome == 'unknown' else _STREAM_ERRORS.get(code, NabedraError)
    raise kind(error['message'], code=code, outcome=outcome,
               request_id=request_id, status_code=status)
