"""Immutable server table handles and single-request row operations."""
from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
import json
from types import MappingProxyType
from typing import Literal
from uuid import UUID

from .client import _Transport
from .errors import LimitExceededError, ProtocolError, ValidationError
from .expr import Expr
from .query import Query
from .validation import (
    Key, Row, Scalar, Schema, schema_from_wire, validate_format,
    validate_identifier, validate_key, validate_row, validate_schema,
)

_MAX_ROWS = 10_000
_MAX_BODY = 8 * 1024 * 1024


def _null_result(data: object) -> None:
    if data is not None:
        raise ProtocolError('Expected a null result')


def _bool_result(data: object) -> bool:
    if type(data) is not bool:
        raise ProtocolError('Expected a boolean result')
    return data


@dataclass(frozen=True, slots=True, eq=False, init=False)
class Table:
    """Construct from a wire descriptor; keep its ID and schema for this handle."""

    id: str
    name: str
    key: str
    format: Literal['parquet']
    _schema: Mapping[str, type]
    _transport: _Transport

    def __init__(self, transport: _Transport, descriptor: object) -> None:
        if type(descriptor) is not dict or set(descriptor) != {'id', 'name', 'schema', 'key', 'format'}:
            raise ProtocolError('Invalid table descriptor fields')
        table_id = descriptor['id']
        if type(table_id) is not str or len(table_id) != 36:
            raise ProtocolError('Table ID must be a hyphenated UUID string')
        try:
            parsed = UUID(table_id)
        except ValueError:
            raise ProtocolError('Invalid table UUID') from None
        if str(parsed) != table_id.lower():
            raise ProtocolError('Table ID must be a hyphenated UUID string')
        name = validate_identifier(descriptor['name'])
        key = validate_identifier(descriptor['key'])
        format = validate_format(descriptor['format'])
        schema = validate_schema(schema_from_wire(descriptor['schema']), key=key)
        for field, value in (('id', table_id), ('name', name), ('key', key), ('format', format),
                             ('_schema', MappingProxyType(schema)), ('_transport', transport)):
            object.__setattr__(self, field, value)

    @property
    def schema(self) -> Schema:
        return dict(self._schema)

    def insert(self, row: Row) -> None:
        checked = validate_row(row, self._schema, key=self.key)
        return self._transport.request('table.insert', {'table_id': self.id, 'row': checked}, decode=_null_result)

    def insert_many(self, rows: Iterable[Row]) -> int:
        # Empty rows already account for both brackets and the entire envelope.
        envelope = {'request_id': '0' * 36, 'operation': 'table.insert_many',
                    'payload': {'table_id': self.id, 'rows': []}}
        size = len(json.dumps(envelope, ensure_ascii=False, allow_nan=False, separators=(',', ':')).encode('utf-8'))
        batch = []
        try:
            iterator = iter(rows)
        except TypeError:
            raise ValidationError('Rows must be iterable') from None
        for row in iterator:
            if len(batch) == _MAX_ROWS:
                raise LimitExceededError('Batch exceeds 10000 rows')
            checked = validate_row(row, self._schema, key=self.key)
            try:
                encoded = json.dumps(checked, ensure_ascii=False, allow_nan=False, separators=(',', ':')).encode('utf-8')
            except (ValueError, TypeError, UnicodeError, RecursionError):
                raise ValidationError('Row must encode as finite UTF-8 JSON') from None
            size += len(encoded) + bool(batch)
            if size > _MAX_BODY:
                raise LimitExceededError('Encoded request exceeds 8 MiB')
            batch.append(checked)

        def count_result(data: object) -> int:
            if type(data) is not int or data != len(batch):
                raise ProtocolError('Inserted count must match submitted batch')
            return data

        return self._transport.request('table.insert_many', {'table_id': self.id, 'rows': batch}, decode=count_result)

    def get(self, key: Key) -> Row | None:
        checked = validate_key(key, self._schema, self.key)

        def row_result(data: object) -> Row | None:
            if data is None:
                return None
            return validate_row(data, self._schema, key=self.key)

        return self._transport.request('table.get', {'table_id': self.id, 'key': checked}, decode=row_result)

    def update(self, key: Key, values: Row) -> bool:
        checked_key = validate_key(key, self._schema, self.key)
        checked_values = validate_row(values, self._schema, key=self.key, partial=True)
        return self._transport.request('table.update', {'table_id': self.id, 'key': checked_key, 'values': checked_values}, decode=_bool_result)

    def upsert(self, row: Row) -> None:
        checked = validate_row(row, self._schema, key=self.key)
        return self._transport.request('table.upsert', {'table_id': self.id, 'row': checked}, decode=_null_result)

    def delete(self, key: Key) -> bool:
        checked = validate_key(key, self._schema, self.key)
        return self._transport.request('table.delete', {'table_id': self.id, 'key': checked}, decode=_bool_result)

    def query(self) -> Query:
        return Query(self)

    def where(self, expr: Expr | None = None, **filters: Scalar) -> Query:
        return self.query().where(expr, **filters)

    def drop(self) -> None:
        return self._transport.request('table.drop', {'table_id': self.id}, decode=_null_result)


def _table_from_descriptor(transport: _Transport, data: object) -> Table:
    """Decode one descriptor inside transport.request's decode callback.

    DB uses decode=lambda data: _table_from_descriptor(transport, data).
    Invalid data raises ProtocolError, ValidationError or LimitExceededError.
    """
    return Table(transport, data)
