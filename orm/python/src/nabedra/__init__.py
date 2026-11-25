"""Nabedra Database public types, errors, table handles and query builders."""
from .errors import (
    AuthenticationError,
    BatchError,
    ConflictError,
    LimitExceededError,
    NabedraError,
    NotFoundError,
    OverloadedError,
    ProtocolError,
    SearchUnavailableError,
    StorageError,
    StreamAbandonedError,
    StreamExpiredError,
    StreamUnsupportedError,
    TransportError,
    UnknownOutcomeError,
    ValidationError,
)
from ._vector import Vector
from .validation import Key, Row, Scalar, Schema
from .expr import Expr, col
from .query import Query
from .stream import BatchStream, StreamSnapshot
from .table import Table
from .client import Database, DB, RequestReceipt
from .cache import Cache
from .cluster import Cluster

__all__ = [
    "Scalar", "Key", "Row", "Schema", "Vector",
    "Cache", "Database", "DB", "RequestReceipt", "Cluster", "Expr", "col", "Query", "Table",
    "NabedraError", "ValidationError", "AuthenticationError", "NotFoundError",
    "ConflictError", "LimitExceededError", "OverloadedError", "StorageError",
    "SearchUnavailableError", "BatchError",
    "TransportError", "ProtocolError", "UnknownOutcomeError",
    "BatchStream", "StreamSnapshot", "StreamUnsupportedError",
    "StreamExpiredError", "StreamAbandonedError",
]

from .serve import serve
from .providers import KubernetesProvider, ProviderError
__all__ += ["serve", "KubernetesProvider", "ProviderError"]

from .aws import AwsCredentials
from .gateway import Gateway, GatewayContext, HTTPResponse, S3
__all__ += ["AwsCredentials", "Gateway", "GatewayContext", "HTTPResponse", "S3"]
