"""Nabedra Database failures and request diagnostics.

An unknown outcome means a mutation may have committed. Inspect current data
before deciding whether to submit another write.
"""
from __future__ import annotations


class NabedraError(Exception):
    """Base failure; local errors can lack request and HTTP status information."""

    code = "nabedra_error"
    _default_outcome: str | None = None

    def __init__(
        self,
        message: str,
        *,
        code: str | None = None,
        request_id: str | None = None,
        status_code: int | None = None,
        outcome: str | None = None,
    ) -> None:
        super().__init__(message)
        self.code = self.code if code is None else code
        self.request_id = request_id
        self.status_code = status_code
        self.outcome = self._default_outcome if outcome is None else outcome


class ValidationError(NabedraError):
    code = "validation_error"
    _default_outcome = "not_applied"


class AuthenticationError(NabedraError):
    code = "unauthorized"
    _default_outcome = "not_applied"


class NotFoundError(NabedraError):
    code = "not_found"
    _default_outcome = "not_applied"


class ConflictError(NabedraError):
    code = "conflict"
    _default_outcome = "not_applied"


class LimitExceededError(NabedraError):
    code = "limit_exceeded"
    _default_outcome = "not_applied"


class OverloadedError(NabedraError):
    code = "overloaded"
    _default_outcome = "not_applied"


class StorageError(NabedraError):
    code = "storage_unavailable"
    _default_outcome = "not_applied"


class TransportError(NabedraError):
    code = "transport_error"


class ProtocolError(NabedraError):
    code = "protocol_error"


class UnknownOutcomeError(NabedraError):
    code = "outcome_unknown"
    _default_outcome = "unknown"


class StreamUnsupportedError(NabedraError):
    """The server or requested query does not support streaming."""

    code = "unsupported_operation"
    _default_outcome = "not_applied"


class StreamExpiredError(NabedraError):
    """A stream or snapshot lease has expired."""

    code = "stream_expired"
    _default_outcome = "not_applied"


class StreamAbandonedError(NabedraError):
    """The worker run that owned a stream or snapshot has ended."""

    code = "stream_abandoned"
    _default_outcome = "not_applied"
