"""Vector and list column types, their checked values and their exact wire form."""
from __future__ import annotations

from array import array
import base64
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
import math
import sys
from typing import Literal, TypeAlias, get_args, get_origin

from .errors import LimitExceededError, ProtocolError, ValidationError

MAX_DIMENSIONS = 4096
MAX_VECTORS = 1024
_MAX_INDEX = 2**32 - 1
_BIG_ENDIAN = sys.byteorder == 'big'
_INT32_CODE = 'I' if array('I').itemsize == 4 else 'L'
_FLOAT32_FORMATS = ('f', '@f', '=f', '>f' if _BIG_ENDIAN else '<f')

Kind: TypeAlias = Literal['dense', 'sparse', 'multi', 'tags']
VectorValue: TypeAlias = (
    Sequence[float] | Sequence[Sequence[float]] | Mapping[int, float] | array | memoryview
)


@dataclass(frozen=True, slots=True)
class Vector:
    """A float32 vector column: Vector(1024), or Vector(sparse=True) for index weights."""

    dim: int | None = None
    sparse: bool = field(default=False, kw_only=True)

    def __post_init__(self) -> None:
        if self.sparse is True and self.dim is None:
            return
        if self.sparse is not False or type(self.dim) is not int or not 1 <= self.dim <= MAX_DIMENSIONS:
            raise ValidationError(f'A vector needs 1 to {MAX_DIMENSIONS} dimensions, or sparse')


def column_kind(typ: object) -> Kind | None:
    """Classify a nonscalar column type; None means a scalar or an unsupported type."""
    if type(typ) is Vector:
        return 'sparse' if typ.sparse else 'dense'
    if get_origin(typ) is list:
        items = get_args(typ)
        if items == (str,):
            return 'tags'
        if len(items) == 1 and type(items[0]) is Vector and not items[0].sparse:
            return 'multi'
    return None


def type_to_wire(typ: object) -> dict:
    kind = column_kind(typ)
    if kind == 'sparse':
        return {'type': 'vector', 'sparse': True}
    if kind == 'dense':
        return {'type': 'vector', 'dim': typ.dim}
    item, = get_args(typ)
    return {'type': 'list', 'item': 'utf8' if item is str else type_to_wire(item)}


def type_from_wire(wire: object) -> object:
    """Decode one nonscalar column type. Unknown or extra fields are errors."""
    if type(wire) is dict and wire.get('type') == 'vector':
        if set(wire) == {'type', 'dim'}:
            return Vector(wire['dim'])
        if set(wire) == {'type', 'sparse'} and wire['sparse'] is True:
            return Vector(sparse=True)
    elif type(wire) is dict and wire.get('type') == 'list' and set(wire) == {'type', 'item'}:
        item = wire['item']
        if item == 'utf8':
            return list[str]
        if type(item) is dict and item.get('type') == 'vector' and set(item) == {'type', 'dim'}:
            return list[Vector(item['dim'])]
    raise ValidationError('Unsupported wire schema type')


def well_formed(value: str) -> bool:
    """A string without lone surrogates, so it encodes as UTF-8."""
    try:
        value.encode('utf-8')
    except UnicodeEncodeError:
        return False
    return True


def text(value: str, column: str) -> str:
    if not well_formed(value):
        raise ValidationError(f'Column {column!r} requires well-formed strings')
    return value


def _float32(values: object, column: str) -> array:
    """Copy Python floats to finite float32 values."""
    if not set(map(type, values)) <= {float, int}:
        raise ValidationError(f'Column {column!r} requires a float32 vector')
    try:
        result = array('f', values)
    except OverflowError:
        result = array('f', [math.inf])
    return _finite(result, column)


def _finite(values: array, column: str) -> array:
    # A float64 sum of finite float32 values cannot overflow.
    if not math.isfinite(sum(values)):
        raise ValidationError(f'Column {column!r} requires finite float32 values')
    return values


def dense(value: object, column: str, dim: int | None = None) -> array:
    if type(value) in (list, tuple):
        result = _float32(value, column)
    else:
        result = array('f')
        try:
            view = memoryview(value)
            if view.format not in _FLOAT32_FORMATS or view.ndim != 1 or not view.contiguous:
                raise TypeError
            result.frombytes(view.tobytes())
        except (TypeError, ValueError, BufferError):
            raise ValidationError(f'Column {column!r} requires a float32 vector') from None
        _finite(result, column)
    if dim is not None and len(result) != dim:
        raise ValidationError(f'Column {column!r} requires {dim} dimensions, got {len(result)}')
    return result


def sparse(value: object, column: str) -> dict[int, float]:
    if not isinstance(value, Mapping):
        raise ValidationError(f'Column {column!r} requires a mapping of index to weight')
    entries = dict(value)
    if any(type(index) is not int or not 0 <= index <= _MAX_INDEX for index in entries):
        raise ValidationError(f'Column {column!r} requires unsigned 32-bit indices')
    if not set(map(type, entries.values())) <= {float, int}:
        raise ValidationError(f'Column {column!r} requires a mapping of index to weight')
    indices = sorted(entries)
    return dict(zip(indices, _float32([entries[index] for index in indices], column)))


def multi(value: object, column: str, dim: int | None = None) -> list[array]:
    if type(value) not in (list, tuple):
        raise ValidationError(f'Column {column!r} requires a sequence of float32 vectors')
    if len(value) > MAX_VECTORS:
        raise LimitExceededError(f'Column {column!r} exceeds {MAX_VECTORS} vectors')
    return [dense(item, column, dim) for item in value]


def tags(value: object, column: str) -> list[str]:
    if type(value) not in (list, tuple) or not set(map(type, value)) <= {str}:
        raise ValidationError(f'Column {column!r} requires a list of strings')
    return [text(item, column) for item in value]


def check_value(value: object, typ: object, column: str) -> object:
    """Copy one row value of a nonscalar column. Only vector columns accept None."""
    kind = column_kind(typ)
    if kind == 'tags':
        return tags(value, column)
    if value is None:
        return None
    if kind == 'dense':
        return dense(value, column, typ.dim)
    if kind == 'sparse':
        return sparse(value, column)
    return multi(value, column, get_args(typ)[0].dim)


def query_value(value: object, column: str) -> str | array | dict[int, float] | list[array]:
    """Detach a query value before its column type is known."""
    if type(value) is str:
        return text(value, column)
    if isinstance(value, Mapping):
        return sparse(value, column)
    if type(value) in (list, tuple) and value and type(value[0]) not in (float, int, bool):
        return multi(value, column)
    return dense(value, column)


def check_query(value: object, typ: object, column: str) -> None:
    """Match a detached query value against its column's type."""
    max_words = 1 << 18
    kind = column_kind(typ)
    if kind == 'dense':
        if type(value) is not array:
            raise ValidationError(f'Column {column!r} requires a float32 vector')
        dense(value, column, typ.dim)
    elif kind == 'sparse':
        if type(value) is not dict:
            raise ValidationError(f'Column {column!r} requires a mapping of index to weight')
        if len(value) > max_words:
            raise LimitExceededError(f'Column {column!r} exceeds {max_words} query vector words')
    elif type(value) is not list:
        raise ValidationError(f'Column {column!r} requires a sequence of float32 vectors')
    else:
        if sum(map(len, value)) > max_words:
            raise LimitExceededError(f'Column {column!r} exceeds {max_words} query vector words')
        multi(value, column, get_args(typ)[0].dim)


def has_zero_vector(value: object) -> bool:
    if type(value) is dict:
        return not any(value.values())
    return not all(map(any, value)) if type(value) is list else not any(value)


def _encode(values: array) -> str:
    if _BIG_ENDIAN:
        values = values[:]
        values.byteswap()
    return base64.b64encode(values).decode('ascii')


def to_wire(value: object, typ: object) -> object:
    """Encode a checked row value. Scalars and lists of strings are already JSON."""
    kind = column_kind(typ)
    if value is None or kind in (None, 'tags'):
        return value
    if kind == 'dense':
        return {'f32': _encode(value)}
    if kind == 'sparse':
        return {'sparse': {'i': _encode(array(_INT32_CODE, value)), 'f32': _encode(array('f', value.values()))}}
    joined = array('f')
    for item in value:
        joined.extend(item)
    return {'f32s': _encode(joined), 'n': len(value)}


def _decode(text: object, code: str) -> array:
    result = array(code)
    try:
        raw = base64.b64decode(text, validate=True)
        if type(text) is not str or base64.b64encode(raw).decode('ascii') != text:
            raise ValueError
        result.frombytes(raw)
    except (TypeError, ValueError):
        raise ProtocolError('Invalid vector encoding') from None
    if _BIG_ENDIAN:
        result.byteswap()
    if code == 'f' and not math.isfinite(sum(result)):
        raise ProtocolError('Nonfinite vector value')
    return result


def _fields(data: object, *names: str) -> list:
    if type(data) is not dict or set(data) != set(names):
        raise ProtocolError('Invalid vector value')
    return [data[name] for name in names]


def from_wire(data: object, typ: object, column: str) -> object:
    """Decode one value of a nonscalar column exactly."""
    kind = column_kind(typ)
    if kind == 'tags':
        if type(data) is not list:
            raise ProtocolError('Invalid list value')
        return tags(data, column)
    if data is None:
        return None
    if kind == 'dense':
        encoded, = _fields(data, 'f32')
        result = _decode(encoded, 'f')
        if len(result) != typ.dim:
            raise ProtocolError('Invalid vector dimensions')
        return result
    if kind == 'sparse':
        pair, = _fields(data, 'sparse')
        encoded, weights = _fields(pair, 'i', 'f32')
        indices, values = _decode(encoded, _INT32_CODE), _decode(weights, 'f')
        if len(indices) != len(values) or any(a >= b for a, b in zip(indices, indices[1:])):
            raise ProtocolError('Invalid sparse vector')
        return dict(zip(indices, values))
    encoded, count = _fields(data, 'f32s', 'n')
    values, dim = _decode(encoded, 'f'), get_args(typ)[0].dim
    if type(count) is not int or not 0 <= count <= MAX_VECTORS or len(values) != count * dim:
        raise ProtocolError('Invalid vector list')
    return [values[start:start + dim] for start in range(0, len(values), dim)]


def base64_overhead(wire: object) -> int:
    """Encoded minus raw bytes of one wire value."""
    if type(wire) is not dict:
        return 0
    parts = wire['sparse'].values() if 'sparse' in wire else (wire.get('f32', ''), wire.get('f32s', ''))
    return sum(len(text) - (len(text) // 4 * 3 - text.count('=', -2)) for text in parts)
