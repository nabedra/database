"""Synchronous, pull-based OLAP query batches and shared snapshot leases."""
from __future__ import annotations

from collections.abc import Callable, Iterator
import re
from types import TracebackType
from typing import TYPE_CHECKING, Literal, Self, TypeVar
import uuid

from ._stream_transport import _StreamChannel, _json_object
from .client import _MAX_BODY
from .errors import LimitExceededError, NabedraError, ProtocolError, StreamUnsupportedError, ValidationError
from .validation import Row, Schema, validate_value

if TYPE_CHECKING:
    from pyarrow import RecordBatch

    from .client import _Transport

_T = TypeVar('_T')
_MAX_BATCH_ROWS = 10_000
_MAX_U64 = (1 << 64) - 1
_MAX_U32 = (1 << 32) - 1
_ARROW_CONTENT = 'application/vnd.apache.arrow.stream'
_JSON_CONTENT = 'application/json'
_ARROW_EOS = b'\xff\xff\xff\xff\x00\x00\x00\x00'
_SAFE_SNAPSHOT_ID = re.compile(r'[A-Za-z0-9][A-Za-z0-9._~-]{0,127}')


def _integer(value: object, minimum: int, maximum: int, name: str) -> int:
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValidationError(f'{name} must be an integer between {minimum} and {maximum}')
    return value


def _uuid(value: object) -> str:
    if type(value) is str:
        try:
            if str(uuid.UUID(value)) == value:
                return value
        except ValueError:
            pass
    raise ProtocolError('Expected a canonical stream UUID')


def _response_object(content_type: str, raw: bytes) -> dict:
    if content_type != _JSON_CONTENT:
        raise ProtocolError('Expected application/json')
    return _json_object(raw)


class _StreamLease:
    """Share capability discovery and lease cleanup between the two handles."""

    def __init__(
        self, transport: _Transport, route: str, id_field: str, parameters: dict,
    ) -> None:
        self._transport = transport
        self._route = route
        self._id_field = id_field
        self._parameters = parameters
        self._started = False
        self.closed = False
        self.channel: _StreamChannel | None = None
        self.id: str | None = None
        self.run_id: str | None = None

    def open(self) -> int:
        if self.closed or self._started:
            raise ValidationError('Stream handles can only be opened once')
        self._started = True
        try:
            return self._open()
        except BaseException:
            self.close()
            raise

    def _open(self) -> int:
        channel = _StreamChannel(self._transport)
        self.channel = channel
        _, content_type, raw = channel.exchange(
            'GET', '/v1/streams/capabilities', expected=(200,), capabilities=True,
        )
        caps = _response_object(content_type, raw)
        if type(caps.get('version')) is not int or caps['version'] != 1:
            if type(caps.get('version')) is int and caps['version'] > 1:
                raise StreamUnsupportedError('Server stream protocol version is unsupported')
            raise ProtocolError('Invalid stream protocol version')
        if set(caps) != {'version', 'run_id', 'max_batch_rows', 'max_batch_bytes', 'max_prefetch_batches'}:
            raise ProtocolError('Invalid streaming capabilities')
        run_id = _uuid(caps['run_id'])
        if (type(caps['max_batch_rows']) is not int or not 0 < caps['max_batch_rows'] <= _MAX_U64
                or type(caps['max_batch_bytes']) is not int or not 0 < caps['max_batch_bytes'] <= _MAX_U64
                or type(caps['max_prefetch_batches']) is not int or not 0 <= caps['max_prefetch_batches'] <= 2):
            raise ProtocolError('Invalid streaming capability limits')
        if self._parameters.get('batch_rows', 0) > caps['max_batch_rows']:
            raise LimitExceededError('batch_rows exceeds the server stream limit')
        request_id = str(uuid.uuid4())
        payload = {
            **self._parameters, 'request_id': request_id, 'expected_run_id': run_id,
        }
        _, content_type, raw = channel.exchange(
            'POST', self._route, expected=(201,), value=payload, request_id=request_id,
        )
        opened = _response_object(content_type, raw)
        if _uuid(opened.get('run_id')) != run_id:
            raise ProtocolError('Stream open returned a different run ID')
        handle_id = opened.get(self._id_field)
        if self._id_field == 'stream_id':
            handle_id = _uuid(handle_id)
        elif type(handle_id) is not str or _SAFE_SNAPSHOT_ID.fullmatch(handle_id) is None:
            raise ProtocolError('Invalid stream snapshot ID')
        self.id, self.run_id = handle_id, run_id
        expected_fields = {self._id_field, 'run_id', 'expires_at'}
        if self._id_field == 'stream_id':
            expected_fields.add('prefetch_batches')
            depth = opened.get('prefetch_batches')
            if type(depth) is not int or not 0 <= depth <= min(
                self._parameters['prefetch_batches'], caps['max_prefetch_batches'],
            ):
                raise ProtocolError('Invalid negotiated prefetch depth')
        if (set(opened) != expected_fields or type(opened['expires_at']) is not int
                or not 0 < opened['expires_at'] <= _MAX_U64):
            raise ProtocolError('Invalid stream open response')
        return min(caps['max_batch_bytes'], _MAX_BODY)

    def path(self) -> str:
        return f'{self._route}/{self.run_id}/{self.id}'

    def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        channel = self.channel
        self.channel = None
        if channel is None:
            return
        try:
            if self.id is not None:
                channel.exchange('DELETE', self.path(), expected=(204,), cleanup=True)
        except NabedraError:
            # A disconnect or unavailable server can defer cleanup until lease expiry.
            pass
        finally:
            channel.close()


class BatchStream(Iterator[_T]):
    """A single-use iterator of batches. A context closes it on early exit."""

    def __init__(
        self,
        lease: _StreamLease,
        *,
        batch_rows: int,
        drop_last: bool,
        limit: int | None,
        content_type: str,
        decode: Callable[[bytes], tuple[_T, int]],
    ) -> None:
        self._lease = lease
        self._batch_rows = batch_rows
        self._drop_last = drop_last
        self._limit = limit
        self._content_type = content_type
        self._decode = decode
        self._next_sequence = 0
        self._delivered_rows = 0
        self._short_tail = False
        self._max_bytes = 0

    def __enter__(self) -> Self:
        self._max_bytes = self._lease.open()
        return self

    def __exit__(
        self, exc_type: type[BaseException] | None,
        exc: BaseException | None, traceback: TracebackType | None,
    ) -> None:
        self.close()

    def __iter__(self) -> Self:
        return self

    def __next__(self) -> _T:
        if self._lease.closed:
            raise StopIteration
        if self._lease.channel is None:
            self.__enter__()
        channel = self._lease.channel
        assert channel is not None
        try:
            if self._next_sequence > _MAX_U64:
                raise ProtocolError('Stream sequence exceeds its wire limit')
            status, content_type, raw = channel.exchange(
                'POST', f'{self._lease.path()}/batches/{self._next_sequence}',
                expected=(200, 204), max_bytes=self._max_bytes,
            )
            if status == 204:
                raise StopIteration
            if content_type != self._content_type:
                raise ProtocolError('Unexpected stream batch content type')
            if self._short_tail:
                raise ProtocolError('Stream produced a batch after its short tail')
            batch, rows = self._decode(raw)
            delivered = self._delivered_rows + rows
            if (not 0 < rows <= self._batch_rows or (self._drop_last and rows != self._batch_rows)
                    or (self._limit is not None and delivered > self._limit)):
                raise ProtocolError('Stream batch violates its row limit')
        except BaseException:
            self.close()
            raise
        self._next_sequence += 1
        self._delivered_rows = delivered
        self._short_tail = rows < self._batch_rows
        return batch

    def close(self) -> None:
        """Release this stream. On HTTP failure the server lease can expire."""
        self._lease.close()


class StreamSnapshot:
    """A pinned query snapshot shared by disjoint shard streams."""

    def __init__(self, lease: _StreamLease) -> None:
        self._lease = lease

    @property
    def id(self) -> str:
        """Opaque snapshot identity, available after entering the context."""
        if self._lease.id is None:
            raise ValidationError('Enter the snapshot context before accessing its ID')
        return self._lease.id

    def __enter__(self) -> Self:
        self._lease.open()
        return self

    def __exit__(
        self, exc_type: type[BaseException] | None,
        exc: BaseException | None, traceback: TracebackType | None,
    ) -> None:
        self.close()

    def close(self) -> None:
        """Close this snapshot to new streams."""
        self._lease.close()


def _rows_decoder(schema: Schema, batch_rows: int) -> Callable[[bytes], tuple[list[Row], int]]:
    def decode(raw: bytes) -> tuple[list[Row], int]:
        result = _json_object(raw)
        if set(result) != {'rows'} or type(result['rows']) is not list:
            raise ProtocolError('Expected a rows batch')
        if not 0 < len(result['rows']) <= batch_rows:
            raise ProtocolError('Invalid stream batch row count')
        rows = []
        for row in result['rows']:
            if type(row) is not dict or row.keys() != schema.keys():
                raise ProtocolError('Stream row must contain exactly the projected columns')
            try:
                rows.append({
                    column: validate_value(row[column], kind, column=column)
                    for column, kind in schema.items()
                })
            except ValidationError:
                raise ProtocolError('Invalid stream row scalar') from None
        return rows, len(rows)

    return decode


def _arrow_decoder(schema: Schema, batch_rows: int) -> Callable[[bytes], tuple[RecordBatch, int]]:
    try:
        import pyarrow as pa
        import pyarrow.compute as pc
    except ImportError:
        raise StreamUnsupportedError('Arrow streaming requires the nabedra[arrow] extra') from None

    types = {str: pa.string(), int: pa.int64(), float: pa.float64(), bool: pa.bool_()}
    expected = pa.schema([pa.field(column, types[kind], nullable=False) for column, kind in schema.items()])

    def decode(raw: bytes) -> tuple[RecordBatch, int]:
        if not raw.endswith(_ARROW_EOS):
            raise ProtocolError('Arrow stream batch is missing its end marker')
        source = pa.BufferReader(raw)
        try:
            with pa.ipc.open_stream(source) as reader:
                if not reader.schema.equals(expected):
                    raise ProtocolError('Arrow batch does not match the projected schema')
                try:
                    batch = reader.read_next_batch()
                except StopIteration:
                    raise ProtocolError('Arrow stream contains no record batch') from None
                if not 0 < batch.num_rows <= batch_rows:
                    raise ProtocolError('Invalid Arrow batch row count')
                batch.validate(full=True)
                for column, kind in zip(batch.columns, schema.values()):
                    if column.null_count:
                        raise ProtocolError('Arrow batch contains null stream values')
                    if kind is float and not pc.all(pc.is_finite(column)).as_py():
                        raise ProtocolError('Arrow batch contains nonfinite stream values')
                try:
                    reader.read_next_batch()
                except StopIteration:
                    if source.tell() != len(raw):
                        raise ProtocolError('Unexpected bytes after Arrow batch end marker') from None
                else:
                    raise ProtocolError('Arrow stream must contain exactly one record batch')
        except (pa.ArrowException, OSError, ValueError):
            raise ProtocolError('Malformed Arrow stream batch') from None
        return batch, batch.num_rows

    return decode


def _batch_stream(
    transport: _Transport, table_id: str, query: dict, schema: Schema, *,
    batch_rows: int, prefetch_batches: int, format: Literal["rows", "arrow"],
    drop_last: bool, snapshot: str | None, shard: int, limit: int | None,
) -> BatchStream[list[Row]] | BatchStream[RecordBatch]:
    batch_rows = _integer(batch_rows, 1, _MAX_BATCH_ROWS, 'batch_rows')
    prefetch_batches = _integer(prefetch_batches, 0, 2, 'prefetch_batches')
    shard = _integer(shard, 0, _MAX_U32, 'shard')
    if type(format) is not str or format not in ('rows', 'arrow'):
        raise ValidationError('Stream format must be rows or arrow')
    if type(drop_last) is not bool:
        raise ValidationError('drop_last must be a bool')
    if snapshot is None:
        if shard != 0:
            raise ValidationError('A nonzero shard requires a shared snapshot ID')
    elif type(snapshot) is not str or _SAFE_SNAPSHOT_ID.fullmatch(snapshot) is None:
        raise ValidationError('Invalid stream snapshot ID')
    lease = _StreamLease(transport, '/v1/streams', 'stream_id', {
        'table_id': table_id, 'query': query, 'batch_rows': batch_rows,
        'prefetch_batches': prefetch_batches, 'format': format,
        'drop_last': drop_last, 'snapshot_id': snapshot, 'shard': shard,
    })
    if format == 'arrow':
        return BatchStream(lease, batch_rows=batch_rows, drop_last=drop_last, limit=limit,
                           content_type=_ARROW_CONTENT, decode=_arrow_decoder(schema, batch_rows))
    return BatchStream(lease, batch_rows=batch_rows, drop_last=drop_last, limit=limit,
                       content_type=_JSON_CONTENT, decode=_rows_decoder(schema, batch_rows))


def _stream_snapshot(transport: _Transport, table_id: str, query: dict, shards: int) -> StreamSnapshot:
    shards = _integer(shards, 1, _MAX_U32, 'shards')
    return StreamSnapshot(_StreamLease(
        transport, '/v1/stream-snapshots', 'snapshot_id',
        {'table_id': table_id, 'query': query, 'shards': shards},
    ))
