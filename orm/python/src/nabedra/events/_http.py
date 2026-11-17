"""Bounded JSON HTTP transport for the Events service."""
from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from email.utils import parsedate_to_datetime
from http.client import HTTPException
import json
import math
import re
import ssl
from typing import TypeVar
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit, urlunsplit
from urllib.request import HTTPRedirectHandler, HTTPSHandler, Request, build_opener

MAX_EVENT_BYTES = 64 * 1024
MAX_BODY_BYTES = 1024 * 1024
MAX_PAGE_COUNT = 100
MAX_OFFSET = 2**64 - 1
MAX_PARTITION = 2**32 - 1
_TOKEN = re.compile(r"[A-Za-z0-9._~+/\-]+=*")
_CODE = re.compile(r"[a-z][a-z0-9_]{0,63}")
_T = TypeVar("_T")


class EventsError(Exception):
    """An Events failure with diagnostics from the broker, when available."""

    def __init__(
        self,
        message: str,
        *,
        code: str,
        status_code: int | None = None,
        retry_after: float | None = None,
        partition: int | None = None,
        earliest_offset: int | None = None,
        outcome: str | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.status_code = status_code
        self.retry_after = retry_after
        self.partition = partition
        self.earliest_offset = earliest_offset
        self.outcome = outcome


class EventsProtocolError(EventsError):
    """A read returned an invalid response."""


class EventsTransportError(EventsError):
    """A read did not receive a complete response."""


class EventsUnknownOutcomeError(EventsError):
    """A write may have committed. Retry only an identity-safe operation."""

    claim_id: str | None = None


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def canonical_json(data: object, *, limit: int = MAX_BODY_BYTES) -> bytes:
    """Encode finite JSON with a bounded output and stable ordering of keys."""
    encoded = bytearray()

    def append(text: str) -> None:
        raw = text.encode("utf-8")
        if len(raw) > limit - len(encoded):
            raise ValueError("Encoded JSON exceeds the byte limit")
        encoded.extend(raw)

    def string(value: str) -> None:
        if len(value) > limit - len(encoded):
            raise ValueError("Encoded JSON exceeds the byte limit")
        append(json.dumps(value, ensure_ascii=False))

    def write(value: object, depth: int) -> None:
        kind = type(value)
        if kind in (dict, list):
            if depth >= 64:
                raise ValueError("JSON nesting exceeds 64 container levels")
            if len(value) > limit - len(encoded):
                raise ValueError("Encoded JSON exceeds the byte limit")
        if kind is dict:
            if any(type(key) is not str for key in value):
                raise ValueError("JSON object keys must be strings")
            append("{")
            for index, key in enumerate(sorted(value)):
                if index:
                    append(",")
                string(key)
                append(":")
                write(value[key], depth + 1)
            append("}")
        elif kind is list:
            append("[")
            for index, child in enumerate(value):
                if index:
                    append(",")
                write(child, depth + 1)
            append("]")
        elif kind is str:
            string(value)
        elif kind is int:
            if not -(2**63) <= value <= MAX_OFFSET:
                raise ValueError("JSON integer does not fit an int64 or uint64")
            append(str(value))
        elif kind is float:
            if not math.isfinite(value):
                raise ValueError("JSON numbers must be finite")
            append(repr(value))
        elif kind is Decimal:
            if not value.is_finite() or not math.isfinite(float(value)):
                raise ValueError("JSON number is outside the supported range")
            append(str(value))
        elif kind is bool:
            append("true" if value else "false")
        elif value is None:
            append("null")
        else:
            raise ValueError("Use JSON values made of dicts, lists and scalars")

    try:
        write(data, 0)
    except UnicodeError:
        raise ValueError("JSON strings must encode as UTF-8") from None
    return bytes(encoded)


def json_loads(raw: bytes, *, exact_numbers: bool = False) -> object:
    def unique_object(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("Duplicate JSON field")
            result[key] = value
        return result

    def invalid_constant(value):
        raise ValueError("Nonfinite JSON number")

    def finite_float(value):
        result = float(value)
        if not math.isfinite(result):
            raise ValueError("Nonfinite JSON number")
        return result

    def exact_decimal(value: str) -> Decimal:
        try:
            result = Decimal(value)
        except InvalidOperation:
            raise ValueError("Invalid JSON number") from None
        if not math.isfinite(float(result)):
            raise ValueError("JSON number is outside the supported range")
        return result

    return json.loads(raw.decode("utf-8"), object_pairs_hook=unique_object,
                      parse_constant=invalid_constant,
                      parse_float=exact_decimal if exact_numbers else finite_float)


def bounded_int(value: object, maximum: int, *, minimum: int = 0) -> int:
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValueError(f"Expected an integer between {minimum} and {maximum}")
    return value


def offset_string(value: object, *, minimum: int = 0) -> int:
    if type(value) is not str or re.fullmatch(r"0|[1-9][0-9]{0,19}", value) is None:
        raise ValueError("Expected a canonical decimal offset string")
    return bounded_int(int(value), MAX_OFFSET, minimum=minimum)


def positive_seconds(value: object) -> float:
    try:
        if type(value) in (int, float) and math.isfinite(value) and value > 0:
            return float(value)
    except OverflowError:
        pass
    raise ValueError("Expected a positive finite duration")


def _retry_after(header: str | None) -> float | None:
    if not header or len(header) > 128:
        return None
    if header.isascii() and header.isdigit():
        return float(header)
    try:
        when = parsedate_to_datetime(header)
        return max(0.0, (when - datetime.now(timezone.utc)).total_seconds())
    except (TypeError, ValueError, OverflowError):
        return None


def _endpoint(url: str, allow_insecure: bool) -> str:
    try:
        if (not isinstance(url, str) or not url
                or any(not 32 < ord(char) < 127 for char in url)
                or any(char in url for char in "?#\\")
                or type(allow_insecure) is not bool):
            raise ValueError
        parts = urlsplit(url)
        schemes = {"http", "https"} if allow_insecure else {"https"}
        if parts.scheme not in schemes or not parts.hostname or parts.username is not None:
            raise ValueError
        parts.port
    except ValueError:
        raise ValueError("Events URL must use HTTPS without credentials, query or fragment "
                         "(HTTP requires allow_insecure=True)") from None
    return urlunsplit((parts.scheme, parts.netloc, parts.path.rstrip("/"), "", ""))


class HTTP:
    def __init__(self, url: str, token: str, *, timeout: float, ca_file: str | None, allow_insecure: bool):
        self._url = _endpoint(url, allow_insecure)
        if not isinstance(token, str) or _TOKEN.fullmatch(token) is None or len(token) > 8192:
            raise ValueError("Expected a nonempty, header-safe bearer token")
        self._token = token
        self._timeout = positive_seconds(timeout)
        if ca_file is not None and (type(ca_file) is not str or not ca_file or "\0" in ca_file):
            raise ValueError("CA file must be a nonempty path string")
        context = ssl.create_default_context()
        if ca_file is not None:
            try:
                context.load_verify_locations(cafile=ca_file)
            except (OSError, ValueError):
                raise ValueError("Could not load Events TLS trust configuration") from None
        self._opener = build_opener(_NoRedirect(), HTTPSHandler(context=context))

    def request(
        self,
        method: str,
        path: str,
        *,
        decode: Callable[[object], _T],
        body: bytes | None = None,
        lease: str | None = None,
        expected: tuple[int, ...] = (200,),
    ) -> _T:
        headers = {"Authorization": f"Bearer {self._token}", "Accept": "application/json"}
        if body is not None:
            headers["Content-Type"] = "application/json"
        if lease is not None:
            headers["X-Events-Lease"] = lease
        request = Request(self._url + path, data=body, headers=headers, method=method)
        writing = method != "GET"
        status: int | None = None

        def invalid(message: str, *, network: bool = False) -> EventsError:
            if writing:
                return EventsUnknownOutcomeError(message, code="outcome_unknown", status_code=status,
                                                 outcome="unknown")
            if network:
                return EventsTransportError(message, code="transport_error", status_code=status)
            return EventsProtocolError(message, code="protocol_error", status_code=status)

        try:
            try:
                response = self._opener.open(request, timeout=self._timeout)
            except HTTPError as error:
                response = error
            with response:
                status = response.status
                raw = response.read(MAX_BODY_BYTES + 1)
                content_type = response.headers.get_content_type()
                length = response.headers.get("Content-Length")
                retry_after = _retry_after(response.headers.get("Retry-After"))
        except (OSError, URLError):
            raise invalid("Events response lost during HTTP I/O", network=True) from None
        except HTTPException:
            raise invalid("Incomplete or invalid Events HTTP response", network=True) from None

        if len(raw) > MAX_BODY_BYTES:
            raise invalid("Events response exceeds the byte limit")
        if length is not None and (not length.isascii() or not length.isdecimal()
                                   or len(length) > 10 or int(length) != len(raw)):
            raise invalid("Invalid or incomplete Events response length", network=True)
        if status == 204 and status in expected and not raw:
            return decode(None)
        if content_type != "application/json":
            raise invalid("Events response is not application/json")
        try:
            value = json_loads(raw, exact_numbers=True)
            if status in expected:
                return decode(value)
            if 400 <= status <= 599:
                raise self._server_error(value, status, retry_after, writing)
        except (ValueError, TypeError, UnicodeError, RecursionError):
            raise invalid("Malformed Events response JSON or fields") from None
        raise invalid("Unexpected Events response status")

    @staticmethod
    def _server_error(
        value: object, status: int, retry_after: float | None, writing: bool
    ) -> EventsError:
        if type(value) is not dict:
            raise ValueError("Expected an Events error object")
        code = value.get("code")
        message = value.get("message")
        outcome = value.get("outcome")
        if (type(code) is not str or _CODE.fullmatch(code) is None
                or type(message) is not str or outcome not in (None, "not_applied", "unknown")):
            raise ValueError("Invalid Events error details")
        partition = value.get("partition")
        if partition is not None:
            partition = bounded_int(partition, MAX_PARTITION)
        earliest = value.get("earliest_offset")
        if earliest is not None:
            earliest = offset_string(earliest, minimum=1)
        if writing and (outcome == "unknown" or code in {"outcome_unknown", "unknown_outcome"}
                        or status in (502, 504) and outcome != "not_applied"):
            kind = EventsUnknownOutcomeError
            outcome = "unknown"
        else:
            kind = EventsError
        return kind(message, code=code, status_code=status, retry_after=retry_after,
                    partition=partition, earliest_offset=earliest, outcome=outcome)
