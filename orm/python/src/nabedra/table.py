"""Immutable server table handles and single-request row operations."""
from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator, Mapping
from dataclasses import dataclass
import json
import time
from typing import Literal, TypeAlias
from uuid import UUID, uuid4

from ._layout import OPTION_FIELDS, Layout, check_embed, extend
from ._vector import Vector, base64_overhead, column_kind, type_to_wire
from .client import _ERROR_TYPES, _Transport
from .errors import (
    BatchError, LimitExceededError, NabedraError, ProtocolError, RowOutcome, UnknownOutcomeError, ValidationError,
)
from .expr import Expr
from .query import Query
from .validation import (
    ColumnType, Key, Row, Scalar, Schema, _decode_row, schema_from_wire, validate_format,
    validate_identifier, validate_key, validate_row, validate_schema,
)

# Fields: state, lag_ms, pending_embeddings, failed_embeddings and recall.
SearchStatus: TypeAlias = dict[str, str | int | float | None]

_MAX_ROWS = 10_000
_MAX_BODY = 8 * 1024 * 1024
_MAX_ROW = 1024 * 1024
_DESCRIPTOR_FIELDS = {'id', 'name', 'schema', 'key', 'format'}
_STATUS_FIELDS = {'state', 'lag_ms', 'pending_embeddings', 'failed_embeddings', 'recall'}


def _null_result(data: object) -> None:
    if data is not None:
        raise ProtocolError('Expected a null result')


def _bool_result(data: object) -> bool:
    if type(data) is not bool:
        raise ProtocolError('Expected a boolean result')
    return data


def _encode(value: object) -> bytes:
    try:
        return json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(',', ':')).encode('utf-8')
    except (ValueError, TypeError, UnicodeError, RecursionError):
        raise ValidationError('Row must encode as finite UTF-8 JSON') from None


def _count(value: object) -> bool:
    return type(value) is int and value >= 0


def _status_result(data: object) -> dict[str, SearchStatus]:
    if type(data) is not dict or set(data) != {'columns'} or type(data['columns']) is not dict:
        raise ProtocolError('Invalid search status')
    for column, status in data['columns'].items():
        validate_identifier(column)
        if (type(status) is not dict or set(status) != _STATUS_FIELDS
                or status['state'] not in ('building', 'ready', 'failed')
                or not (status['lag_ms'] is None or _count(status['lag_ms']))
                or not _count(status['pending_embeddings']) or not _count(status['failed_embeddings'])
                or not (status['recall'] is None or type(status['recall']) is float)):
            raise ProtocolError('Invalid search status')
    return data['columns']


def _row_outcome(data: object, key: Key, request_id: str) -> RowOutcome:
    if data == {'o': 'applied'}:
        return {'key': key, 'outcome': 'applied', 'error': None}
    if type(data) is not dict or set(data) != {'o', 'error'} or data['o'] not in ('not_applied', 'outcome_unknown'):
        raise ProtocolError('Invalid row outcome')
    error, unknown = data['error'], data['o'] == 'outcome_unknown'
    if (type(error) is not dict or set(error) != {'code', 'message', 'outcome'}
            or type(error['code']) is not str or not error['code'] or type(error['message']) is not str
            or error['outcome'] != ('unknown' if unknown else 'not_applied')):
        raise ProtocolError('Invalid row outcome')
    kind = UnknownOutcomeError if unknown else _ERROR_TYPES.get(error['code'], NabedraError)
    return {'key': key, 'outcome': data['o'], 'error': kind(
        error['message'], code=error['code'], request_id=request_id, outcome=error['outcome'])}


@dataclass(frozen=True, slots=True, eq=False, init=False)
class Table:
    """Construct from a wire descriptor; keep its ID and schema for this handle."""

    id: str
    name: str
    key: str
    format: Literal['parquet']
    _layout: Layout
    _transport: _Transport

    def __init__(self, transport: _Transport, descriptor: object) -> None:
        if (type(descriptor) is not dict or not _DESCRIPTOR_FIELDS <= set(descriptor)
                or not set(descriptor) <= _DESCRIPTOR_FIELDS.union(OPTION_FIELDS)):
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
        layout = Layout.from_descriptor(schema, {field: descriptor[field] for field in OPTION_FIELDS
                                                 if field in descriptor})
        for field, value in (('id', table_id), ('name', name), ('key', key), ('format', format),
                             ('_layout', layout), ('_transport', transport)):
            object.__setattr__(self, field, value)

    @property
    def schema(self) -> Schema:
        return dict(self._layout.schema)

    def _row(self, row: Row, *, partial: bool = False) -> dict:
        """Check a row against the writable columns and return its wire form."""
        layout = self._layout
        if isinstance(row, Mapping):
            for column in layout.embed:
                if column in row:
                    raise ValidationError(f'Column {column!r} is embedded and cannot be written')
        wire = layout.row_to_wire(validate_row(row, layout.written, key=self.key, partial=partial))
        if layout.has_search and len(_encode(wire)) - sum(map(base64_overhead, wire.values())) > _MAX_ROW:
            raise LimitExceededError('Row exceeds 1 MiB')
        return wire

    def insert(self, row: Row) -> None:
        checked = self._row(row)
        return self._transport.request('table.insert', {'table_id': self.id, 'row': checked}, decode=_null_result)

    def insert_many(self, rows: Iterable[Row]) -> int:
        # Empty rows already account for both brackets and the entire envelope.
        envelope = {'request_id': '0' * 36, 'operation': 'table.insert_many',
                    'payload': {'table_id': self.id, 'rows': []}}
        size = len(_encode(envelope))
        batch = []
        try:
            iterator = iter(rows)
        except TypeError:
            raise ValidationError('Rows must be iterable') from None
        for row in iterator:
            if len(batch) == _MAX_ROWS:
                raise LimitExceededError('Batch exceeds 10000 rows')
            checked = self._row(row)
            size += len(_encode(checked)) + bool(batch)
            if size > _MAX_BODY:
                raise LimitExceededError('Encoded request exceeds 8 MiB')
            batch.append(checked)

        def count_result(data: object) -> int:
            if type(data) is not int or data != len(batch):
                raise ProtocolError('Inserted count must match submitted batch')
            return data

        return self._transport.request('table.insert_many', {'table_id': self.id, 'rows': batch}, decode=count_result)

    def get(self, key: Key) -> Row | None:
        layout = self._layout
        checked = validate_key(key, layout.schema, self.key)

        def row_result(data: object) -> Row | None:
            return None if data is None else _decode_row(data, layout.schema)

        return self._transport.request('table.get', {'table_id': self.id, 'key': checked}, decode=row_result)

    def update(self, key: Key, values: Row) -> bool:
        checked_key = validate_key(key, self._layout.schema, self.key)
        checked_values = self._row(values, partial=True)
        return self._transport.request('table.update', {'table_id': self.id, 'key': checked_key, 'values': checked_values}, decode=_bool_result)

    def upsert(self, row: Row) -> None:
        checked = self._row(row)
        return self._transport.request('table.upsert', {'table_id': self.id, 'row': checked}, decode=_null_result)

    def delete(self, key: Key) -> bool:
        checked = validate_key(key, self._layout.schema, self.key)
        return self._transport.request('table.delete', {'table_id': self.id, 'key': checked}, decode=_bool_result)

    def upsert_many(self, rows: Iterable[Row], *, wait_for_embedding: bool = False) -> int:
        """Write rows in bounded requests and return how many were applied.

        Each row is atomic and durable. The call is not atomic across rows. A failure
        stops the call with BatchError, and nothing is sent again.
        """
        if type(wait_for_embedding) is not bool:
            raise ValidationError('wait_for_embedding must be a bool')
        if wait_for_embedding and not self._layout.embed:
            raise ValidationError('Waiting for embeddings requires an embedded column')

        def check(row: Row) -> tuple[Key, object]:
            checked = self._row(row)
            return checked[self.key], checked

        applied = self._write_many('table.upsert_many', 'rows', rows, check)
        if wait_for_embedding and applied:
            self._wait_for_embedding(applied)
        return applied

    def delete_many(self, keys: Iterable[Key]) -> int:
        """Delete keys in bounded requests. A missing key counts as applied."""
        def check(key: Key) -> tuple[Key, object]:
            checked = validate_key(key, self._layout.schema, self.key)
            return checked, checked

        return self._write_many('table.delete_many', 'keys', keys, check)

    def _write_many(self, operation: str, field: str, items: Iterable,
                    check: Callable[[object], tuple[Key, object]]) -> int:
        chunks = self._chunks(operation, field, items, check)
        applied = 0
        while True:
            try:
                chunk = next(chunks, None)
            except Exception as error:
                if not applied:
                    raise
                raise BatchError(f'Batch stopped after {applied} applied rows: {error}',
                                 applied=applied, outcome='partial') from error
            if chunk is None:
                return applied
            applied += self._send_chunk(operation, field, chunk, applied)

    def _chunks(self, operation: str, field: str, items: Iterable,
                check: Callable[[object], tuple[Key, object]]) -> Iterator[dict[Key, object]]:
        # The empty chunk already accounts for both brackets and the entire envelope.
        base = len(_encode({'request_id': '0' * 36, 'operation': operation,
                            'payload': {'table_id': self.id, field: []}}))
        try:
            iterator = iter(items)
        except TypeError:
            raise ValidationError('Rows must be iterable' if field == 'rows' else 'Keys must be iterable') from None
        chunk: dict[Key, object] = {}
        size = base
        for item in iterator:
            key, checked = check(item)
            encoded = len(_encode(checked))
            if base + encoded > _MAX_BODY:
                raise LimitExceededError('Encoded request exceeds 8 MiB')
            if len(chunk) == _MAX_ROWS or size + encoded + bool(chunk) > _MAX_BODY:
                yield chunk
                chunk, size = {}, base
            if key in chunk:
                raise ValidationError('A key appears more than once')
            size += encoded + bool(chunk)
            chunk[key] = checked
        if chunk:
            yield chunk

    def _send_chunk(self, operation: str, field: str, chunk: dict[Key, object], applied: int) -> int:
        request_id, cause = str(uuid4()), None

        def outcomes_result(data: object) -> list[RowOutcome]:
            if (type(data) is not dict or set(data) != {'outcomes'} or type(data['outcomes']) is not list
                    or len(data['outcomes']) != len(chunk)):
                raise ProtocolError('Expected one outcome for each row')
            return [_row_outcome(item, key, request_id) for item, key in zip(data['outcomes'], chunk)]

        try:
            outcomes = self._transport.request(
                operation, {'table_id': self.id, field: list(chunk.values())},
                decode=outcomes_result, request_id=request_id)
        except NabedraError as error:
            cause = error
            outcome = 'not_applied' if error.outcome == 'not_applied' else 'outcome_unknown'
            outcomes = [{'key': key, 'outcome': outcome, 'error': error} for key in chunk]
        rejected = [row for row in outcomes if row['outcome'] == 'not_applied']
        unknown = [row for row in outcomes if row['outcome'] == 'outcome_unknown']
        done = len(outcomes) - len(rejected) - len(unknown)
        if done == len(outcomes):
            return done
        applied += done
        raise BatchError(
            f'Batch stopped after {applied} applied rows: {len(rejected)} not applied, {len(unknown)} unknown',
            applied=applied, outcomes=outcomes, request_id=request_id,
            outcome='unknown' if unknown else 'partial' if applied else 'not_applied',
        ) from (cause or (rejected + unknown)[0]['error'])

    def _wait_for_embedding(self, applied: int) -> None:
        deadline, pause, cause = time.monotonic() + self._transport._timeout, 0.05, None
        while (remaining := deadline - time.monotonic()) > 0:
            try:
                status = self.search_status()
            except NabedraError as error:
                cause = error
                break
            if all(column in status and status[column]['pending_embeddings'] == 0 for column in self._layout.embed):
                return
            time.sleep(min(pause, remaining))
            pause = min(pause * 2, 1.0)
        raise BatchError(f'{applied} rows were applied and their embeddings can still be pending',
                         applied=applied, outcome='applied') from cause

    def add_column(self, name: str, type: Vector | list[Vector], *,
                   embed: Mapping[str, str] | None = None) -> None:
        """Add a vector column online. This handle then uses the new schema."""
        options = None if embed is None else check_embed(embed, self._layout.schema)
        layout = extend(self._layout, name, type, options)
        added = layout.schema[name]
        payload = {'table_id': self.id, 'name': name, 'type': type_to_wire(added)}
        if options is not None:
            payload['embed'] = options
        self._change('table.add_column', payload, lambda changed: changed.schema.get(name) == added
                     and changed.embed.get(name) == options)

    def drop_column(self, name: str) -> None:
        """Drop a vector column. This handle then uses the new schema."""
        if column_kind(self._known(name)) not in ('dense', 'sparse', 'multi'):
            raise ValidationError(f'Column {name!r} is not a vector column')
        self._change('table.drop_column', {'table_id': self.id, 'name': name},
                     lambda changed: name not in changed.schema)

    def add_full_text(self, column: str, language: str) -> None:
        """Index a str column for match(), in the given language."""
        self._known(column)
        layout = self._layout
        if column in layout.full_text:
            raise ValidationError(f'Column {column!r} already has full text')
        Layout.build(layout.schema, layout.partition_by, {**layout.full_text, column: language}, layout.embed)
        self._change('table.add_full_text', {'table_id': self.id, 'column': column, 'language': language},
                     lambda changed: changed.full_text.get(column) == language)

    def drop_full_text(self, column: str) -> None:
        """Stop indexing a str column for match()."""
        self._known(column)
        if column not in self._layout.full_text:
            raise ValidationError(f'Column {column!r} has no full text')
        self._change('table.drop_full_text', {'table_id': self.id, 'column': column},
                     lambda changed: column not in changed.full_text)

    def _known(self, column: str) -> ColumnType:
        validate_identifier(column)
        if column not in self._layout.schema:
            raise ValidationError(f'Unknown column {column!r}')
        return self._layout.schema[column]

    def _change(self, operation: str, payload: dict, applied: Callable[[Layout], bool]) -> None:
        def layout_result(data: object) -> Layout:
            table = Table(self._transport, data)
            if ((table.id.lower(), table.name, table.key) != (self.id.lower(), self.name, self.key)
                    or not applied(table._layout)):
                raise ProtocolError('Changed descriptor does not match the request')
            return table._layout

        layout = self._transport.request(operation, payload, decode=layout_result)
        object.__setattr__(self, '_layout', layout)

    def search_status(self) -> dict[str, SearchStatus]:
        """Report state, lag, embedding counts and recall of each search column."""
        return self._transport.request('table.search_status', {'table_id': self.id}, decode=_status_result)

    def query(self) -> Query:
        return Query(self)

    def where(self, expr: Expr | None = None, **filters: Scalar) -> Query:
        return self.query().where(expr, **filters)

    def drop(self) -> None:
        return self._transport.request('table.drop', {'table_id': self.id}, decode=_null_result)


def _table_from_descriptor(transport: _Transport, data: object) -> Table:
    """Decode one descriptor inside transport.request's decode callback.

    Database uses decode=lambda data: _table_from_descriptor(transport, data).
    Invalid data raises ProtocolError, ValidationError or LimitExceededError.
    """
    return Table(transport, data)
