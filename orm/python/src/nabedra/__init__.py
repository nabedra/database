"""Nabedra Database public types, errors, table handles and query builders."""
from .errors import (
    AuthenticationError,
    ConflictError,
    LimitExceededError,
    NabedraError,
    NotFoundError,
    OverloadedError,
    ProtocolError,
    StorageError,
    StreamAbandonedError,
    StreamExpiredError,
    StreamUnsupportedError,
    TransportError,
    UnknownOutcomeError,
    ValidationError,
)
from .validation import Key, Row, Scalar, Schema
from .expr import Expr, col
from .query import Query
from .stream import BatchStream, StreamSnapshot
from .table import Table
from .client import Database, DB, RequestReceipt
from .cluster import Cluster

__all__ = [
    "Scalar", "Key", "Row", "Schema",
    "Database", "DB", "RequestReceipt", "Cluster", "Expr", "col", "Query", "Table",
    "NabedraError", "ValidationError", "AuthenticationError", "NotFoundError",
    "ConflictError", "LimitExceededError", "OverloadedError", "StorageError",
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
