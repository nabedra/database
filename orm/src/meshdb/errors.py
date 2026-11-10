"""MeshDB failures and request diagnostics.

An unknown outcome means a mutation may have committed. Inspect current data
before deciding whether to submit another write.
"""
from __future__ import annotations


class MeshDBError(Exception):
    """Base failure; local errors can lack request and HTTP status information."""

    code = "meshdb_error"
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


class ValidationError(MeshDBError):
    code = "validation_error"
    _default_outcome = "not_applied"


class AuthenticationError(MeshDBError):
    code = "unauthorized"
    _default_outcome = "not_applied"


class NotFoundError(MeshDBError):
    code = "not_found"
    _default_outcome = "not_applied"


class ConflictError(MeshDBError):
    code = "conflict"
    _default_outcome = "not_applied"


class LimitExceededError(MeshDBError):
    code = "limit_exceeded"
    _default_outcome = "not_applied"


class OverloadedError(MeshDBError):
    code = "overloaded"
    _default_outcome = "not_applied"


class StorageError(MeshDBError):
    code = "storage_unavailable"
    _default_outcome = "not_applied"


class TransportError(MeshDBError):
    code = "transport_error"


class ProtocolError(MeshDBError):
    code = "protocol_error"


class UnknownOutcomeError(MeshDBError):
    code = "outcome_unknown"
    _default_outcome = "unknown"
